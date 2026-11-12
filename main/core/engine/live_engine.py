"""LiveEngine — 实盘端到端引擎（0009 切片4）。

定时拉 Bar（BarPoller）→ Portfolio.on_bar 取信号/风控 → HttpBridgeDispatcher 真实下单
→ 落 live_orders/live_trades。引擎核心（Portfolio/ExecutionEngine）复用回测逻辑，
仅注入 HttpBridgeDispatcher + LiveT1Checker 替换 Simulated*。

成交时机：当根 bar 立即成交（实盘无「下一 bar」可等），桥受理即视为成交，
成交价首期用 bar.close（OrderEvent.price），真实价切片5 /deals 回填。

持仓恢复（recover）：Core 重启后从 live_trades 重放，重建 StrategyContext.positions
与 Account.cash，保证恢复后的持仓结构与在线时一致。虚拟现金以成本计（§93）。
"""
import asyncio
import functools
import logging
import math
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP
from typing import (
    AsyncIterator, Callable, Dict, List, Optional, Set, Tuple,
)

from sqlalchemy import func, or_
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import DetachedInstanceError

from .portfolio import Portfolio
from .position import Position
from .format import dec_money, dec_price, fmt_money, fmt_price
from .execution_engine import ExecutionEngine, LiveT1Checker
from .event import BarEvent, OrderEvent, TradeEvent
try:
    from .http_bridge_dispatcher import (
        HttpBridgeDispatcher,
        BridgeUnavailableError,
        BridgeOrderRejected,
        BridgeOrderIdError,
    )
except Exception:
    class BridgeUnavailableError(RuntimeError):
        pass

    class BridgeOrderRejected(RuntimeError):
        pass

    class BridgeOrderIdError(BridgeOrderRejected):  # type: ignore
        pass

    HttpBridgeDispatcher = object  # type: ignore
from .bar_poller import (
    BarPoller,
    QuotePollingWindow,
    latest_completed_bar,
    parse_bar_time,
    to_ohlcv,
)
# TradingCalendar 为 live 专属（仅实盘引擎及其协作者使用，回测不依赖），
# 0010 后续迁入 live 子包；旧路径 core.engine.trading_calendar 保留 re-export shim。
from .live.calendar import TradingCalendar
from .live.snapshot_bars import SnapshotBarSynthesizer
# 时间/周期/数值工具归 live 协作者子包（core.engine.live.timing，0010 步骤 0）；
# re-export 其中被测试直接 import 的符号（now_shanghai/periods_on_boundary/_CST），
# 保持 core.engine.live_engine 命名空间可见。
from .live.timing import (
    _CST,
    _parse_insert_utc,
    now_shanghai,
    periods_on_boundary,
)
from .live.event_bus import EventBus
# 外部信号注入源（通达信条件预警文件 → 公式名匹配策略 → 注入 signal_cache）
from .live.external_signal_source import (
    ExternalSignalFileSource,
    normalize_external_symbol,
)
from .live.context import EngineContext
from .live.market_data import (
    MarketDataService,
    _FORMULA_META_KEYS,
    _STARTUP_ONLY_PERIODS,
)
from .live.order_machine import (
    OrderStateMachine,
    detach_loaded,
    _ORDER_REF_MATCH_TIMEOUT,
    _ORDER_INSERT_TOLERANCE,
    _ORDER_STATUS_FILLED,
    _ORDER_STATUS_PARTIAL_CANCELED,
    _ORDER_STATUS_CANCELED,
    _ORDER_STATUS_JUNK,
    _ORDER_STATUS_TERMINAL_CANCELED,
    _ORDER_STATUS_TERMINAL_REJECTED,
    _UNTRACEABLE_MARKER,
    _MANUAL_REVIEW_MARKER,
)
from .live.breaker import BreakerService
from .live.daily_closer import DailyCloser
from .market_rules import session_allows_bar, tick_size
from .decision import DecisionRecorder
from .regime import CausalRegimeTracker
from ..config import load_config
from core.models import (
    LiveAccountPositionSnapshot, LiveAccountSnapshot, LiveCashFlow,
    LiveDecisionEvent, LiveBarOrderMark, LiveOrder, LivePositionPeak,
    LiveSignal, LiveTrade,
)
from core.tq.formula import TQFormula
from core.tq.utils import TDXLockTimeout
from tq_iquant_shared.constants import SignalType, TradeType

logger = logging.getLogger(__name__)

# 外部预警信号的方向分类：买入类走原开仓闸门（资金/槽位/单仓/敞口/现金），
# 卖出类跳过仓位与资金检查，但受「无持仓不卖」「T+1 可用不足拒单」约束。
# 其它 signal_type 视为不可解析动作：忽略 + 节流告警，绝不猜方向、不默认买入。
_BUY_SIGNAL_TYPES = (SignalType.OPEN, SignalType.ADD)
_SELL_SIGNAL_TYPES = (SignalType.REDUCE, SignalType.CLOSE)
# 不可解析动作告警节流：每 50 条打印一次（计数从 1 开始，首条即告警）。
_UNPARSEABLE_WARN_EVERY = 50
# 盘前放行截止余量（2026-11-11）：桥通道 30s（一次 HTTP 往返 + 受理确认足够）；
# UI 自动化通道按 25s/单估算（点字段 + 键入价 + 等 4s 防覆盖 + 弹窗处置），
# 下限 120s、上限 240s——窗口 09:15–09:20 共 300s，上限避免把窗口压没。
_PRE_OPEN_BRIDGE_MARGIN = 30
_PRE_OPEN_UI_MARGIN_MIN = 120
_PRE_OPEN_UI_MARGIN_MAX = 240
_PRE_OPEN_UI_PER_ORDER = 25
# P2-4：盘前单「开盘越价校验」的重试截止（上海时间）。首根 1m bar 迟到、个别标的
# 缺 open、撤单一时未收敛时保留后续 1m bar 的重试机会；10:00 之后开盘价对撤单决策
# 已无意义，停止重试并记 risk 事件（action=skip/rule=open_check_timeout）。
_PRE_OPEN_OPEN_CHECK_DEADLINE = (10, 0)
# P2-6：桥离线恢复补派的拉取根数区间。断线 3 分钟 → ~8 根；30 分钟 → ~35 根；
# 上午断到下午（跨午休）按墙钟分钟放大但超过 240 根（约一个交易日）就封顶，
# 更早的 bar 不再尝试（跨日缺口直接不做补派，见 _plan_gap_recovery）。
_GAP_BACKFILL_MIN_BARS = 10
_GAP_BACKFILL_MAX_BARS = 240
# 日线及以上周期（与 Portfolio._DAILY_PERIODS 同口径）：这类轮次驱动日线策略，
# 峰值更新允许；盘中的分钟 bar 对「全为日线策略」的组合只是噪声（P1-2）。
_DAILY_PERIODS = ("1d", "1w", "1mon")


def _allow_peak_update(portfolio: Portfolio, bar: BarEvent) -> bool:
    """P1-2：本 bar 是否允许抬净值峰值（回撤分母）。

    对齐回测的峰值更新频率：回测按策略自身 bar 更新（日线策略每日一次、分钟策略
    每根一次）。实盘日线策略额外收到 1m bar，若也抬峰会让回撤分母偏高 → 比回测
    更易熔断，且熔断后「次日恢复」也难回正。规则：
    - 日线及以上轮次（15:05 收盘落定 / 14:50 提前收盘）→ 抬峰；
    - 组合内存在分钟周期策略 → 抬峰（分钟回测本就每根更新，保持两端一致）；
    - 其余（全是日线及以上策略的盘中分钟 bar）→ 不抬峰，只刷新 last_bar/prev_close
      与熔断次日恢复判断（daily_loss 基准不受影响）。
    """
    period = (getattr(bar, "period", "") or "1m").lower()
    if period in _DAILY_PERIODS:
        return True
    return any(
        (getattr(s, "period", "") or "1m").lower() not in _DAILY_PERIODS
        for s in getattr(portfolio, "strategies", [])
    )


def _finite_or_none(value):
    """非有限浮点（inf / -inf / nan / DBL_MAX 哨兵）→ None，其它类型原样返回。

    桥侧已做清洗，这里是落库/回写路径的兜底：QMT 对停牌或异常持仓会返回
    inf（实测 601989 market_value=Infinity），一旦进库，会话详情接口的
    json.dumps 会直接 500（Out of range float values are not JSON compliant）。
    """
    if value is None or isinstance(value, bool):
        return value
    try:
        num = float(value)
    except (TypeError, ValueError):
        return value
    return num if math.isfinite(num) else None


def _allocate_proportional(weights: list, total: int) -> list:
    """按权重把 total 拆成整数（最大余数法），保证各项之和恰为 total。

    多组合策略共享同一 QMT 账户、各维护虚拟持仓：桥只给账户合计数量，
    镜面覆盖时按各策略既有持仓比例分摊，避免某一策略被整仓覆盖吞掉。
    """
    base = sum(weights)
    if not weights or base <= 0 or total <= 0:
        return [0 for _ in weights]
    raw = [w * total / float(base) for w in weights]
    alloc = [int(x) for x in raw]
    remainder = total - sum(alloc)
    order = sorted(range(len(weights)),
                   key=lambda i: (raw[i] - alloc[i], weights[i]), reverse=True)
    for i in range(remainder):
        alloc[order[i % len(order)]] += 1
    return alloc


def _expected_minute_labels(start: Optional[datetime],
                            end: Optional[datetime]) -> List[datetime]:
    """(start, end] 内属于 A 股连续竞价时段的 1m bar 标签（P2-6 缺口审计用）。

    bar 标签是分钟**终点**（09:31 表示 09:30–09:31 这根）：只统计
    09:30 < t ≤ 11:30 与 13:00 < t ≤ 15:00。跨日返回空（跨日缺口不做逐分钟
    期望，调用方按「跨日」语义记录起止即可）。上限 300 根防异常输入死循环。

    P1-2（2026-11-12）：`end` 所在那一分钟是**正在形成**的 bar（其完成时刻 =
    下一分钟整点），不能计入「应有分钟」——否则每轮审计必缺 1 根、`action`
    恒为 `partial`，告警失真。故先回退 1 秒再取分钟下限（`end=10:06:00` →
    `stop=10:05`；`end=10:05:30` → `stop=10:05`）。
    """
    if start is None or end is None or start.date() != end.date():
        return []
    labels: List[datetime] = []
    t = start.replace(second=0, microsecond=0) + timedelta(minutes=1)
    stop = (end - timedelta(seconds=1)).replace(second=0, microsecond=0)
    while t <= stop and len(labels) <= 300:
        hm = (t.hour, t.minute)
        if (9, 30) < hm <= (11, 30) or (13, 0) < hm <= (15, 0):
            labels.append(t)
        t += timedelta(minutes=1)
    return labels


def _position_code(row) -> str:
    """真实持仓行的代码归一：symbol 优先，桥通道回退 instrument+exchange。

    TDX/THS 的 get_positions 返回带后缀 symbol；QMT/掘金桥 /positions 行
    没有 symbol，只有 instrument（601827）与 exchange（SH）。只认 symbol
    会让导入/快照把全部行静默丢掉（2026-11-06 实证：桥 15 只持仓、导入 0 只、
    工作台持仓表为空）。口径与 _intraday_reconcile_diffs/_reconcile_positions
    一致：instrument.exchange。
    """
    if not isinstance(row, dict):
        return ""
    code = str(row.get("symbol") or "").strip()
    if code:
        return code
    inst = str(row.get("instrument") or "").strip()
    exch = str(row.get("exchange") or "").strip()
    if inst and exch:
        return "%s.%s" % (inst, exch)
    return ""


def _position_cost(row, volume: int) -> Decimal:
    """导入仓成本价：行内 cost 优先；cost 字段缺失时才回退「市值÷股数」；否则 0=未知。

    口径（2026-11-10 B5 定稿）：
    - 行的 cost 字段**存在且 > 0** → 直接用（券商合并均价，唯一风控基准）；
    - cost 字段存在但 **<= 0 或为 None**（送转/分红后成本为负，实测 300413=-475.00、
      300722=-10.93；或柜台/桥取不到成本，桥给 None）→ 返回 Decimal("0") 表示
      **成本未知**，**绝不**回退「市值÷股数」——那等于拿现价冒充成本：盈亏恒 0、
      风控失效，且成本随价格每轮漂移（36.430→36.360）。风控见 avg_cost<=0 跳过该票；
    - cost 字段**缺失**（旧桥/通道不给该列）且市值可用 → 「市值÷股数」估值兜底；
    - 两者都拿不到（停牌票 QMT 市值 Infinity→None）→ 0（未知）。
    """
    if "cost" in row:
        # 显式带 cost 列（含 None）→ 只信这一列。桥取不到成本时给的是 None
        # （QMT 客户端无可用成本属性），语义是「未知」而不是「没有这一列」：
        # 回退「市值÷股数」= 拿现价冒充成本（盈亏恒 0、成本每轮漂移、风控假触发）。
        raw = row.get("cost")
        if raw is None:
            return Decimal("0")
        try:
            cost = Decimal(str(raw))
        except Exception:  # noqa: BLE001
            return Decimal("0")
        if cost != cost:  # NaN 守卫
            return Decimal("0")
        return cost if cost > 0 else Decimal("0")
    try:
        mv = Decimal(str(row.get("market_value") or 0))
        vol = Decimal(int(volume))
    except Exception:  # noqa: BLE001
        return Decimal("0")
    if mv > 0 and vol > 0:
        return mv / vol
    return Decimal("0")


def _is_share_count_change(before_qty: int, before_cost: Decimal,
                           qty: int, cost: Decimal) -> bool:
    """数量增加但「总成本基本不变」→ 送转/拆股等股本变动，不是外部真买入。

    券商合并均价在送转后按新股数摊薄：``cost × qty ≈ before_cost × before_qty``
    （总成本守恒）；外部买入必然抬高总成本（≈新买入金额）。因此只有数量增加且
    总成本不增时才判为股本变动。

    保守条件：两侧成本都有效（>0）才判定。任一未知（柜台负成本/0）一律按
    「外部买入」处理——宁可多扣现金，也不让虚拟净值凭空虚增。
    容差 0.5%：送转的总成本是守恒的（差异只来自浮点），真实买入的最小增量
    （1 手）相对大额持仓可能很小，容差过大会把真买入误判成送转。
    """
    if before_qty <= 0 or qty <= before_qty or before_cost <= 0 or cost <= 0:
        return False
    return (cost * Decimal(int(qty))
            <= before_cost * Decimal(int(before_qty)) * Decimal("1.005"))


def _position_market_value(row, volume: int, cost: Decimal) -> Decimal:
    """真实持仓行市值：market_value 优先，缺失时按 cost×volume 估算，都无 → 0。

    用于「池内继承仓位从起账现金扣除」的扣减（起账现金 = 该组合配置资金，2026-11-11
    P1-①）。池内继承仓是配置资金已变成的持仓，只镜像数量而不扣虚拟现金会让净值 =
    起账现金 + 继承市值 → 双计（2026-11-07 实证：基准 27 万 + 继承市值 5.59 万 →
    净值 32.59 万，20% 单仓 6.52 万而非 5.46 万）。池外仓不走此扣减（P1-②）。"""
    try:
        mv = Decimal(str(row.get("market_value") or 0))
    except Exception:  # noqa: BLE001
        mv = Decimal("0")
    if mv > 0:
        return mv
    if cost and cost > 0 and volume > 0:
        return cost * Decimal(int(volume))
    return Decimal("0")


# 深交所收盘时点（上海 15:00）。收盘后禁止新下单——报不进交易所，会成待报单
# 冻结持仓永不成交（真机 2026-08-14 id40-41，bar_time=15:02:42）。
# 用 bar.bar_time 判定（非墙钟：不依赖本机时钟/时区）。仅拦新落单，信号仍推。
_MARKET_CLOSE_TIME = (15, 0)
# 盘中持仓对账节流（秒）：快照采集 5s/60s 级，对账判定 5 分钟一次足够
_INTRADAY_RECONCILE_INTERVAL = 300.0
# 人工干预（柜台被人手动动过平台账内仓）的观测闸门名：只打标 + 落审计事件，
# 不阻断交易、不改变风控优先级（判定与修正见 _note_manual_intervention /
# _detect_counter_cleared_holdings / _apply_close_counter_correction）。
_MANUAL_INTERVENTION_GATE = "manual_intervention"
# 柜台持仓行「连续缺失」确认次数：桥/TQ 单次返回不完整时不得据此清零虚拟账本
# （清仓是记账动作），连续两次采集都缺才算真清仓（会话内 60s 级采集）。
_COUNTER_ABSENT_CONFIRM = 2
# 展示层事件去重表上限（条）：同一 (bar_time, 组合, 策略, 票, 闸门/信号) 只推一次，
# 溢出按插入序淘汰最旧，长跑会话不无限增长（不参与任何交易判断）。
_EVENT_DEDUPE_MAX = 5000
# D 恢复期告警阈值：一次 recover 重建的 deferred 日线单超过该条数 → risk 事件提示
# 人工确认（不阻断启动）。正常单日单组合是「几只票 × 一个方向」级别，两位数意味着
# 日线轮被重复驱动或旧版本重复行未清理（2026-11-04 实证 6 条重复止损卖单）。
DEFERRED_RECOVER_ALERT_THRESHOLD = 10
# deferred 放行宽限（自然日）：正常应于 signal_bar_date 的下一交易日放行。日历
# fail-open 年份（未收录休市表且桥不可用）会把节后首个交易日误算成节中工作日，
# 导致 due 偏早、节前 deferred 单在节后首日被判过期取消。给 7 天宽限兜住长假。
_DEFERRED_RELEASE_GRACE_DAYS = 7
# 可重排拒单的重试上限（P1-3/P1-4，2026-11-11）：涨跌停拦截、桥明确拒单、盘前越价
# 撤单都不再一次性作废，而是回 deferred 等**次一交易日 due 窗口 / 同一交易日 09:31
# 首根 1m bar** 再试；首报 + 2 次重排 = 最多 3 次尝试，仍失败才落终态 rejected。
# 与「每单每日只尝试一次」不冲突：重排不改变当日已尝试的事实，绝不当日重报。
_DEFERRED_RETRY_MAX = 2
# 重排次数落库标记（写 live_orders.error_message，与 deferred_extended: 同惯例，
# 不新增模型列/迁移）。格式：deferred_retry:n=<次数>; <原因>
_RETRY_MARKER = "deferred_retry:n="
# 拒单归因（P1-3）：用户口径是「只有参数错误/资金不足/通道不支持这类真错误才终态」，
# 故默认**可重排**，命中下列真错误关键词才终态。
_REJECT_TERMINAL_HINTS = (
    "资金不足", "可用资金", "可用不足", "可用数量", "可用股份",
    "数量非法", "委托数量", "价格非法", "非交易时间", "不在交易时间",
    "停牌", "重复委托", "invalid", "not supported", "不支持",
    "order confirm dialog", "missing order", "requires price", "exceeds",
)
# 次日盘前（集合竞价）执行窗口：交易日 09:15（开盘集合竞价开始）至结束时间
# （默认 09:20，之后进入不可撤单撮合段，不再新报）。左闭右开，按 (时, 分) 判定。
_PRE_OPEN_START = (9, 15)
_PRE_OPEN_DEFAULT_END = (9, 20)
# 盘前委托价让价幅度默认值（%，相对信号价；见 PreOpenAuctionConfig 注释）。
_PRE_OPEN_DEFAULT_PRICE_BIAS_PCT = Decimal("2")
# 成交价偏离监控默认阈值（%）：桥通道回填的成交价相对审批价（委托价）绝对偏离超过
# 该值即发 risk 事件。配置 config.live.fill_price_deviation_pct 覆盖，<=0 关闭。
_DEFAULT_FILL_PRICE_DEVIATION_PCT = Decimal("1.0")


def _fill_price_deviation_limit(value):
    """config.live.fill_price_deviation_pct → Decimal 阈值；<=0/非法回退默认。

    None 表示调用方未注入：读 load_config()（缺键/异常都回退默认），保证测试可注入、
    且默认开启（偏离无人感知本身就是这次要修的问题）。
    """
    if value is None:
        try:
            value = (load_config().get("live") or {}).get("fill_price_deviation_pct")
        except Exception:  # noqa: BLE001 配置不可读 -> 用默认
            value = None
    if value is None:
        return _DEFAULT_FILL_PRICE_DEVIATION_PCT
    try:
        pct = Decimal(str(value))
    except Exception:  # noqa: BLE001
        return _DEFAULT_FILL_PRICE_DEVIATION_PCT
    if pct != pct:  # NaN 守卫：Decimal("nan") 可构造，比较恒 False
        return _DEFAULT_FILL_PRICE_DEVIATION_PCT
    return pct if pct > 0 else None


def _is_retryable_reject(exc) -> bool:
    """P1-3：该拒单是否可重排（回 deferred，等下一窗口再试）。

    默认**可重排**（涨跌停拦截、桥业务拒单、柜台临时不可用都属「等一下可能就成了」），
    只有真错误才终态：参数错误、资金/可用不足、数量/价格非法、停牌、非交易时间、
    重复委托、通道不支持，以及认领键生成失败（BridgeOrderIdError——重排只会持续撞
    同一配置缺陷，必须人工处理）。与回测 `deferred.append(order)` 同口径。
    """
    if isinstance(exc, BridgeOrderIdError):
        return False
    text = str(exc or "").lower()
    return not any(h.lower() in text for h in _REJECT_TERMINAL_HINTS)


def _retry_count_from_message(message) -> int:
    """从 live_orders.error_message 读回重排次数（recover 跨重启仍受上限约束）。"""
    text = str(message or "")
    if _RETRY_MARKER not in text:
        return 0
    tail = text.split(_RETRY_MARKER, 1)[1]
    digits = ""
    for ch in tail:
        if ch.isdigit():
            digits += ch
        else:
            break
    try:
        return int(digits) if digits else 0
    except ValueError:
        return 0


class PreOpenAuctionConfig:
    """次日盘前（集合竞价）执行日线级信号单的配置（config.tq_trade.pre_open_auction*）。

    默认开启（对齐回测「次日开盘执行」口径）：enabled=False 时引擎完全不进入盘前
    路径，行为与旧版完全一致（deferred 单只由「次一交易日首根完成 1m bar」放行，
    即约 09:31）。config.yaml 写 ``pre_open_auction_enabled: false`` 即可关闭。

    开启后仅在交易日 09:15 ≤ now < end_time 对「当日 due 的 deferred 日线信号
    单」直接报单到柜台集合竞价。盘前没有开盘价，委托价用「信号价（前一日
    deferred 单记录的 price，即信号 bar 收盘）× (1 ± bias%)」并在当日涨跌停
    之内收 1 跳：买 = min(信号价×(1+bias), 涨停价−1 跳)，卖 =
    max(信号价×(1−bias), 跌停价+1 跳)。取 1 跳是为绕开本地涨跌停校验
    （买 ≥ 涨停 / 卖 ≤ 跌停一律拦），而不是把单打到涨跌停板上去追价。
    """

    def __init__(self, enabled: bool = False, end_time=None,
                 price_bias_pct=None):
        self.enabled = bool(enabled)
        self.end_time = self._parse_clock(end_time, _PRE_OPEN_DEFAULT_END)
        try:
            bias = Decimal(str(price_bias_pct))
            self.price_bias_pct = bias if bias >= 0 else Decimal("0")
        except Exception:  # noqa: BLE001
            self.price_bias_pct = _PRE_OPEN_DEFAULT_PRICE_BIAS_PCT

    @classmethod
    def from_config(cls, cfg=None) -> "PreOpenAuctionConfig":
        """config.tq_trade 段 → 配置。缺键/非 dict → 开启（默认对齐回测）。"""
        if not isinstance(cfg, dict):
            cfg = {}
        return cls(
            enabled=cfg.get("pre_open_auction_enabled", True),
            end_time=cfg.get("pre_open_auction_end_time"),
            price_bias_pct=cfg.get("pre_open_auction_price_bias_pct"),
        )

    @staticmethod
    def _parse_clock(value, default: Tuple[int, int]) -> Tuple[int, int]:
        text = str(value or "").strip()
        if not text:
            return default  # 未配置 → 静默用默认
        try:
            hour, minute = text.split(":", 1)
            h, m = int(hour), int(minute)
            if 0 <= h <= 23 and 0 <= m <= 59:
                return (h, m)
        except Exception:  # noqa: BLE001
            pass
        logger.warning("盘前竞价结束时间非法 %r，回退 %02d:%02d", value, *default)
        return default

    def is_open(self, now: datetime) -> bool:
        """now 是否在盘前执行窗口内（不含交易日判定，由调用方叠加）。"""
        if not self.enabled:
            return False
        return _PRE_OPEN_START <= (now.hour, now.minute) < self.end_time


# _DAILY_CLOSE_TIME(14:30) / _DAILY_BARS_TIME(15:05) / _MARKET_CLOSE_SWEEP_TIME(15:05)
# 与日终/收盘编排一并迁入 core.engine.live.daily_closer.DailyCloser（0010 步骤 5）；
# 收盘新落单拦截仍留本模块（_MARKET_CLOSE_TIME，_handle_bar 内按 bar_time 判定）。

# 委托状态机/成交回填相关常量（_ORDER_REF_MATCH_TIMEOUT 等）与 OrderStateMachine 一并
# 迁入 core.engine.live.order_machine（0010 步骤 3），此处由顶部 import re-export，保持
# core.engine.live_engine 命名空间可见。


class LiveEngine:
    def __init__(
        self,
        session_id: int,
        portfolios: List[Portfolio],
        dispatcher: HttpBridgeDispatcher,
        bar_poller: BarPoller,
        db_session_factory: Callable[[], Session],
        poll_interval: float = 60.0,
        deals_poll_interval: float = 5.0,
        stream_ping_interval: float = 30.0,
        tq_formula: Optional[TQFormula] = None,
        formula_by_strategy: Optional[Dict[int, str]] = None,
        formula_count: int = 200,
        formula_count_by_name: Optional[Dict[str, int]] = None,
        code_period_count: Optional[Dict[tuple, int]] = None,
        trading_calendar: Optional[TradingCalendar] = None,
        regime_provider: Optional[Callable] = None,
        benchmark_codes: Optional[Dict[int, str]] = None,
        execution_mode: str = "auto",
        signal_monitor=None,
        signal_scheduler=None,
        formula_id_by_strategy: Optional[Dict[int, int]] = None,
        enable_signal_monitor: bool = False,
        # 外部信号注入（0011）：通达信条件预警文件路径。None=不启用；非空=建
        # ExternalSignalFileSource，_tick_main 每轮增量读取并经 _handle_bar 全套注入。
        signal_file_path: Optional[str] = None,
        # 14:50 提前收盘价（B，可选增强，仅 1d）：默认关 → 1d/1w/1mon 都在 15:05
        # 收盘落定后用完整 bar 算信号、次日开盘执行。开启后仅 1d 提前：用快照合成
        # 14:50 1m bar 收盘价定信号，委托价取 14:52 起始 1m bar 开盘价（14:51 兜底），
        # **该轮信号当日提交、尾盘成交**（不受 risk_timing=next_day 顺延）。
        # 1w/1mon 不提前（仍 15:05 收盘落定 + 次日执行）；仅 auto 模式；回测不读。
        advance_close_enabled: bool = False,
        advance_close_time: str = "14:51",
        # 盘中 1m 数据源（快照合成）：None=不启用（K 线轮询原行为）。TQ 通道
        # （tdx/ths/alert）盘中分钟 K 线只到上一交易日，快照合成器随 deals 节拍
        # 采样、经 SnapshotQuoteDispatcher 供 poller 读取。
        snapshot_synth: Optional[SnapshotBarSynthesizer] = None,
        # 账户全貌对齐：engine_key（快照表落库标记）、account_snapshot_interval
        # （采集节拍秒，0=禁用；live_service 按通道传 5/60）、pool_codes_by_portfolio
        # （组合→股票池代码集，外部持仓归属匹配用）。
        engine_key: str = "",
        account_snapshot_interval: float = 0.0,
        pool_codes_by_portfolio: Optional[Dict[int, set]] = None,
        # 行情轮询窗口（config.live.quote_polling）：交易日窗口外（15:05 收盘后 /
        # 次日 09:15 前 / 非交易日）完全停 BarPoller.poll()，消除收盘后对掘金桥
        # /quote 与通达信 TQ 的无谓压力。None → 安全默认（窗口 09:15–15:05，收盘后停）。
        quote_polling: Optional[Dict] = None,
        # 次日盘前（集合竞价）执行日线级信号单（config.tq_trade.pre_open_auction*）。
        # None/缺键 → 默认开启；显式 false 关闭，完全走「次日首根完成 1m bar 放行」。
        pre_open_auction: Optional[Dict] = None,
        # 错周期 bar 是否跑风控（会话级三态已在 live_service 解析为布尔）：
        # False（默认，对齐回测）→ 错周期 bar 只更新价格快照、不跑风控；
        # True → 旧行为（任何 bar 都按 close 评估持仓风险）。
        intraday_mismatched_risk_enabled: bool = False,
        # 09:31 deferred 兜底放行「保证成交」开关（可复现优先，默认关）：
        # False → 限价 = 当日首根 1m bar 开盘价；True → 通道默认对手价优先成交。
        deferred_release_ensure_fill: bool = False,
        # 成交价偏离监控阈值（%，config.live.fill_price_deviation_pct）：
        # None → 引擎内读配置（缺键/异常回退默认 1%，<=0 关闭）。
        fill_price_deviation_pct: Optional[float] = None,
        # 会话级「真实账户总资产」（live_sessions.live_capital，可选）：只作**真实侧
        # 总闸**的兜底上限（桥账户快照缺失时），**不再**覆盖各组合的虚拟起账资金
        # ——分账不分池（2026-11-11 P1-①）：每个组合起账 = 该组合配置资金。
        account_total_capital: Optional[Decimal] = None,
    ):
        # 交易日历（下单总闸）：默认桥 xtdata 权威日历，桥离线时 TradingCalendar
        # fail-open（工作日放行），测试可注入假日历。
        self._calendar = trading_calendar or TradingCalendar(dispatcher.query_calendar)
        self._bar_poller = bar_poller
        # 行情轮询窗口：窗口外整轮停行情拉取（账户快照/成交回填/收盘清扫不受影响）
        self._quote_polling = QuotePollingWindow.from_config(quote_polling)
        # P2-10：轮询窗口收盘端跟随日历——半日市 11:30 收盘后不再空拉快照
        # （常态日日历给 15:00，加原 5 分钟缓冲 = 15:05，旧行为逐字不变）。
        self._quote_polling.close_time_provider = (
            lambda d: self._calendar.session_close_time(d))
        # 盘前竞价执行窗口（默认开启）+ 当日已尝试集合（at-most-once 幂等）
        self._pre_open = PreOpenAuctionConfig.from_config(pre_open_auction)
        # 错周期 bar 风控（False=只更新价格快照，对齐回测）
        self._intraday_mismatched_risk = bool(intraday_mismatched_risk_enabled)
        # 09:31 deferred 兜底放行定价：False=首根 1m open 限价；True=对手价保成交
        self._deferred_release_ensure_fill = bool(deferred_release_ensure_fill)
        # 盘前已报出单（开盘后越价校验用）：[{day, code, side, limit, strategy_id, order}]
        self._pre_open_open_checks: List[dict] = []
        # 越价校验「当日已完成」标记：P2-4 起只在**真正完成比对**（每单有开盘价、
        # 撤单已收敛）或到达重试截止（10:00）后才置位；缺价/取数失败不置位，
        # 保留后续 1m bar 的重试机会（旧实现先置位后比对，首根 bar 迟到即漏撤）。
        self._pre_open_open_check_day: Optional[date] = None
        # P1-1（2026-11-12）：`_check_pre_open_orders_at_open` 由 `_handle_bar` 按
        # **组合**调用（一个组合一份 bar），同一根 1m bar 会被调 P 次 → 未收敛单
        # 每根 bar 被撤 P 次、`action=cancel` 事件重复 P 条。记录已处理过的 bar
        # 时间，同一根 bar 只跑一次（跨组合共享）。
        self._pre_open_open_check_bar: Optional[datetime] = None
        self._pre_open_day: Optional[date] = None
        self._pre_open_attempted: Set[tuple] = set()
        # 盘前能力闸拦截（QMT 桥不能确认指定价）：按日只记一条
        # gate=pre_open_auction/action=block 风险事件，避免 60s 主循环每轮刷屏。
        self._pre_open_blocked_day: Optional[date] = None
        # 盘前放行截止（2026-11-11）：窗口结束 - 通道余量。过点不再放新单（按日一条
        # gate=pre_open_auction/action=block 事件，rule=release_deadline）。
        self._pre_open_deadline_day: Optional[date] = None
        # 本次放行使用的截止时刻（只读留痕，写进每单 [pre-open] release 日志）。
        self._pre_open_release_cutoff: Optional[datetime] = None
        # 盘前单回显异常（2026-11-10）：仅盘前单的 /order 应答回显非 limit 时记录，
        # 后续盘前单整日拦截（盘中单用配置默认 price_type，回显与盘前无关）。
        self._pre_open_echo_bad: Optional[str] = None
        # 通道认领异常回调（TDX PRICE_MY 价差不符 → risk/claim_price_mismatch）。
        # 只挂给声明了该属性的通道（TdxTqDispatcher）；桥/THS 不受影响。
        # 用构造参数 dispatcher（self.ctx 尚未建立，self._dispatcher 属性不可用）。
        if hasattr(dispatcher, "on_claim_anomaly"):
            dispatcher.on_claim_anomaly = self._on_claim_anomaly
        # 成交价偏离监控（桥通道）：阈值 Decimal（None=关闭）+ 已检查去重集合
        # （键 (order_id, filled_quantity)，同一单的增量成交只在量变化时复查一次）
        self._fill_price_deviation_pct = _fill_price_deviation_limit(
            fill_price_deviation_pct)
        self._fill_dev_checked: Set[Tuple[int, int]] = set()
        # 暂停状态标记：只在进入/退出暂停时各打一条 INFO，避免每 tick 刷屏
        self._quote_poll_paused = False
        # 提交前价格刷新「1m 行情非当日」告警去重（键 (code, bar 时间)）：同一根
        # 陈旧 bar 只 WARN 一次，避免 deferred 批量放行时刷屏
        self._stale_quote_warned: Optional[Set[Tuple[str, str]]] = None
        # 快照合成器（盘中 1m 数据源，TQ 通道专用；None=不启用）
        self._snapshot_synth = snapshot_synth
        # 14:50 提前收盘价（仅 1d）：原始配置，构造 DailyCloser 时再按 auto 模式收敛
        self._advance_close_enabled_cfg = bool(advance_close_enabled)
        self._advance_close_time_cfg = advance_close_time or "14:51"
        # 账户全貌对齐（快照采集 + 外部持仓导入 + BUY 资金对齐）
        self._engine_key = engine_key
        self._acct_snap_interval = account_snapshot_interval
        self._last_acct_capture = 0.0
        self._last_intraday_reconcile = 0.0
        self._latest_account_cash: Optional[Decimal] = None
        # 最新账户快照的采集时刻（上海 naive，与 LiveAccountSnapshot.captured_at 同口径）。
        # 用于判定哪些在途 BUY 已被柜台快照的「可用」扣过冻结：提交时间早于该时刻的
        # 单，快照可用里已含其冻结，不能再从真实侧重复扣（2026-11-10 001311 实证：
        # 56,115 → 5,155.30，3500 股被压到 300 股）。None=不可知 → 退回保守全额扣减。
        self._latest_account_cash_at: Optional[datetime] = None
        # ③ 柜台现金 vs 虚拟现金偏差告警节流（monotonic 秒）：只告警不改账
        self._last_cash_diff_at: float = 0.0
        # ⑥ 真实账户总资产 vs 虚拟净值：最近一次比对结果（会话详情展示）+
        # 每会话只告警一次（>20% 视为账本与真实账户失同步，需人工核对）
        self._latest_equity_check: Optional[dict] = None
        self._equity_deviation_warned: bool = False
        # 无快照采集时间时走保守口径的日志去重（避免逐单刷屏）
        self._conservative_reserve_logged = False
        # 真实账户总资产兜底下限（live_capital）：仅当桥账户快照不可用时作真实侧
        # 总闸上限，**不**参与各组合虚拟起账（分账不分池）。
        self._account_total_capital = (
            Decimal(str(account_total_capital))
            if account_total_capital is not None else None)
        # 「真实侧按资金占比缩量」事件节流：同一 (组合, 交易日) 只记一条
        self._cash_share_scale_days: Dict[tuple, date] = {}
        self._pool_codes_by_portfolio = pool_codes_by_portfolio or {}
        # 池外持仓上报去重（同一批代码只报一次，避免 5s 节拍刷屏）
        self._out_of_pool_reported: set = set()
        # 池外持仓汇总决策事件（live_decision_events）去重：每会话每日最多一条
        self._out_of_pool_event_day: Optional[date] = None
        # 外部持仓快照（只读展示用）：真实账户行中未进入任何虚拟账本的部分
        # （池外仓/无策略组合/在途 BUY 覆盖），每轮采集刷新，由 /positions 合并返回。
        # 不进风控、不占槽位、平台不代卖——绝不是虚拟持仓的备份。
        self._external_positions: List[dict] = []
        # 无可归属组合策略的跳过上报去重（同批代码只报一次；配置恢复后重置）
        self._no_attr_reported: set = set()
        # 零股残行跳过日志去重：首次出现/N 变化报 INFO，其余同值降 DEBUG
        # （None=上次无零股行，再次出现按首次报 INFO）
        self._zero_skipped_reported: Optional[int] = None
        # 真实持仓行缺可用代码（symbol/instrument+exchange 都没有）的告警去重：
        # 一次采集节拍报一次 WARN，恢复后重置，避免 5s/60s 刷屏
        self._no_code_rows_reported = False
        # 导入仓成本未知（桥未给 cost 且市值不可用）的告警去重：每代码一次
        self._no_cost_warned: set = set()
        # 送转/拆股（数量增、合并均价摊薄、总成本未增）识别日志去重：每代码一次
        self._share_change_noted: set = set()
        # max_positions 缺失（该策略未配置）告警去重：每 (组合, 策略) 一次
        self._slot_limit_warned: set = set()
        # 协作者共享状态容器（0010 步骤 2a）：session_id/portfolios/dispatcher/
        # db_session_factory/code_period_count/clock 归 EngineContext，引擎上的同名
        # 属性保留为 property 委托（见类体下方），调用点与测试直连无需改动。
        self.ctx = EngineContext(
            session_id=session_id,
            portfolios=portfolios,
            dispatcher=dispatcher,
            db_session_factory=db_session_factory,
            code_period_count=code_period_count,
            clock=now_shanghai,
        )
        # alert/auto mode
        self.execution_mode = execution_mode
        self._formula_id_by_strategy = formula_id_by_strategy or {}
        self._tq_alert = None
        self._signal_monitor = signal_monitor
        if enable_signal_monitor and signal_monitor is None:
            from ..tq.alert import TQAlert
            from .live.signal_monitor import SignalMonitor
            self._tq_alert = TQAlert()
            self._signal_monitor = SignalMonitor(
                session_id, db_session_factory, self._tq_alert, self._emit,
                execution_mode,
            )
        # 外部信号注入源（0011）：signal_file_path 非空时启用——定时增量读取同花顺
        # 条件预警文件，公式名匹配策略后合成 bar 注入 signal_cache，经 _handle_bar
        # 全套执行（风控/审批/在途闸不绕过）。None=不启用，行为不变。
        self._signal_source: Optional[ExternalSignalFileSource] = None
        if signal_file_path:
            formula_names = {
                name for name in (formula_by_strategy or {}).values() if name
            }
            self._signal_source = ExternalSignalFileSource(
                signal_file_path, formula_names)
            logger.info(
                "external signal source enabled: %s (formulas %s)",
                signal_file_path, sorted(formula_names),
            )
        # 外部信号源跨日重置记账（audit Fix1）：长跑会话跨日不重置则游标停在
        # 昨日、方向去重集合也残留。记引擎启动日，_tick_external_signals 检测
        # 日期变更时清空 (代码, 方向) 去重集合并调 source.reset_daily()（游标
        # 归位当前时间）。
        self._ext_sig_last_date: date = now_shanghai().date()
        # 外部信号方向去重：同代码 + 同日 + 同方向各认第一条（买入/卖出互不
        # 吞并）；元素为 (symbol, "buy"|"sell")。方向由策略公式信号配置决定
        # （预警文件不含方向列）。
        self._ext_seen: Set[Tuple[str, str]] = set()
        # 不可解析动作（未知 signal_type）计数：每 50 条 warning 一次。
        self._ext_unparseable_count = 0
        # 决策闸门采集（调参可观测性）：引擎级单例，注入执行引擎与各组合（set_recorder
        # 顺带透传到 risk_manager）。_handle_bar 每组合末尾 drain 落 LiveDecisionEvent + SSE。
        self._recorder = DecisionRecorder()
        for p in portfolios:
            p.set_recorder(self._recorder)
        # 方案 E（阶段二 A）：持仓峰值逐 bar 持久化。缓冲只在「峰值被抬高」时写入，
        # 由 _flush_position_peaks 在 bar 收尾/快照采集/recover 统一 UPSERT 到
        # live_position_peaks；回测不注入 hook，零影响。_peak_rows = 已知在表里的
        # (组合, 代码) 键，用于清仓删行与空转早退。
        self._peak_buffer: Dict[Tuple[int, str], Tuple[Decimal, Optional[datetime]]] = {}
        self._peak_rows: Set[Tuple[int, str]] = set()
        self._peak_lock = threading.Lock()
        for p in portfolios:
            p.peak_persist_hook = self._note_peak_raised
        self._poll_interval = poll_interval
        # B5 市场状态闸（默认关）：regime_provider(code, end_date) → {date: Decimal}
        # 基准收盘全量历史；每组合每日首个 bar 重建因果跟踪器并注入
        # portfolio.current_regime。provider 为 None（默认）时完全不启用，行为不变。
        self._regime_provider = regime_provider
        self._benchmark_codes: Dict[int, str] = benchmark_codes or {}
        self._regime_updated: Dict[int, date] = {}
        # R3：每组合每日首根 bar 已调 enter_day（跨日补算上一日 daily_loss，
        # 与回测对齐——暂停/恢复在日首生效），防混合周期多 bar/日重复调用。
        self._enter_day_updated: Dict[int, date] = {}
        # trading_session（full/am/pm）时段闸：每组合每日首次跳过记 1 条决策
        # 事件防刷屏（内存 set 按 (portfolio_id, date)，Core 重启重记无碍）。
        self._session_skip_recorded: set = set()
        # 事件级去重（进度/推送噪声收敛，不影响交易决策与下单）：
        # daily_closer 用同一份日线快照依次驱动 1d/1w/1mon，每次 _handle_bar 都对
        # 全部持仓跑风控 → 同一 (bar_time, 票, 规则) 的 decision/signal 会重复三轮。
        # 落单侧已有 deferred/在途/同 bar 同向去重，这里只收敛「决策事件落库 +
        # SSE 推送」。键含 bar_time，跨 bar 天然不同；上限按插入序淘汰防长跑膨胀。
        self._emitted_event_keys: "OrderedDict[tuple, None]" = OrderedDict()
        # B-1 未来函数闸门（对齐回测）：日线级信号当日不下单，订单暂存此内存队列，
        # 次一交易日首 bar（bar_time.date() 严格小于当前 bar 日期）放行重走提交链。
        # 持久化（P2，2026-11-09 强化）：DB 是权威足迹——defer 时落 status=deferred
        # 行；recover 只读不删（行 id 挂 order._deferred_row_id）；放行提交成功时
        # 删旧行建 submitted 新行；stop() 兜底把无行单重落库。任何时刻重启/掉电都
        # 能从 DB 重建队列，不再出现「已 recover 到内存、DB 已无行」的丢失窗口。
        self._signal_bar_deferred: List[OrderEvent] = []
        # 批次1 3a 撤单追价重下：次数记账（引擎内存，重启清零可接受——
        # requote_max_attempts 次数上限兜底，重启后最坏多追价一轮）。
        # key=(strategy_id, stock_code, signal_name, bar_date)，value=已用重试次数。
        self._requote_attempts: Dict[tuple, int] = {}
        # 撤单收敛统一 requote 挂点的防重 set（只触发一次）：条目两种形状——
        #   (strategy_id, stock_code, signal_name, bar_date, attempts)：同键同次数
        #   防重复触发；(“order”, order_id)：同一收敛单只进一次判定（同一单的收敛
        # 事件可能同时出现在主循环消失路径与 deals 池 53/54 终态路径的输入里）。
        self._requote_fired: set = set()
        # 批次2：TDX 大额拆单队列（成交驱动）。key=(strategy_id, stock_code)，
        # value={"inflight": OrderEvent 当前在途子单, "pending": [OrderEvent 未提交子单],
        #        "portfolio": Portfolio, "period": str}。子单 1 落库提交后入队，
        # 由 _tick_split_children（deals 回填循环末尾）在前子单成交后逐笔续发。
        # 仅内存：Core 重启即丢（未提交子单不落库无从重建；已落库子单由 D6 去重
        # 防重放）。注意与 _signal_bar_deferred 不同——后者已 DB 持久化可重建。
        self._split_children: Dict[tuple, dict] = {}
        # G5：/deals 成交回报轮询独立节拍（默认 5s，比主循环 60s 更短）——成交秒级回报，
        # 持仓/资金反馈要近实时，不跟 bar 拉取同频。
        self._deals_poll_interval = deals_poll_interval
        # 实盘执行引擎：复用回测 ExecutionEngine，注入桥 dispatcher + 实盘 T+1 检查
        # F5：t1_checker 持每 bar 刷新的桥可用表（SELL 减仓上限用 m_nCanUseVolume）
        self._t1_checker = LiveT1Checker()
        self._engine = ExecutionEngine(dispatcher, self._t1_checker, recorder=self._recorder)
        # 执行契约文件交接层（file_handoff_enabled 门控，通道无关）：signal/decision/
        # risk 事件同步落 {work_dir}/{session_id}/ 下 signals/risk.jsonl，供外部
        # 诊断/对账。orders.jsonl 由 dispatcher 专职写（避免双写）；写失败不影响主流程。
        self._file_handoff = None
        if getattr(dispatcher, "file_handoff_enabled", False):
            try:
                from .live.file_handoff import FileHandoff
                self._file_handoff = FileHandoff(
                    getattr(dispatcher, "_work_dir", "data/ths"), session_id)
            except Exception:  # noqa: BLE001
                logger.warning("file handoff init failed", exc_info=True)

        # 行情/信号协作者（0010 步骤 2b）：预热/增量 bar 缓存、公式信号注入、周期边界
        # 分发、1w/1mon 通达信注入归 MarketDataService。on_bar 回调注入 self._handle_bar
        # （服务不反向 import LiveEngine，周期/日终 bar 经此回调交回引擎下单/风控）；
        # F5 可用持仓写回经 set_available_map 窄回调。引擎保留同名薄委托方法/property，
        # 既有调用点与测试直连（engine.signal_cache={...}、engine._preheat() 等）穿透。
        self.market_data = MarketDataService(
            self.ctx,
            bar_poller,
            on_bar=(
                self._handle_alert_bar
                if execution_mode == "alert" else self._handle_bar
            ),
            set_available_map=self._t1_checker.set_available_map,
            tq_formula=tq_formula,
            formula_by_strategy=formula_by_strategy,
            formula_count=formula_count,
            formula_count_by_name=formula_count_by_name,
            # 周期边界新鲜度闸（5m/15m/30m/1h）跳过时发 risk 事件（gate=period_bar_freshness）
            emit_risk=self._emit_risk_safe,
        )
        self._signal_scheduler = signal_scheduler
        if (enable_signal_monitor and execution_mode == "alert"
                and signal_scheduler is None):
            from .live.signal_scheduler import SignalScheduler
            self._signal_scheduler = SignalScheduler(
                self.ctx, self.market_data, lambda: self._calendar,
                self.market_data._formula_by_strategy,
                self.market_data._formula_count_by_name,
                self._formula_id_by_strategy,
                self._signal_monitor, self._tq_alert,
            )
        # 委托状态机/成交回填协作者（0010 步骤 3）：order_ref 匹配、/deals 轮询回填、
        # 终态同步、陈旧单失效、filled apply 归 OrderStateMachine。持有 _pending_orders/
        # _last_backfill_time。事件发射经 self._emit 回调注入（不反向 import LiveEngine）。
        # 引擎保留全部同名方法/属性为薄委托，既有测试直连穿透。
        self._order_machine = OrderStateMachine(
            self.ctx, self._emit, resolve_attribution=self._resolve_attribution)
        # 熔断编排协作者（0010 步骤 4）：每 bar update_peak/max_drawdown、日终 daily_loss、
        # 计数持久化、3 次转手动、手动恢复、重启读回归 BreakerService。持有 _breaker_count_written。
        # 事件发射经 self._emit 回调注入（不反向 import LiveEngine）。引擎保留同名薄委托。
        self._breaker = BreakerService(self.ctx, self._emit)
        # 日终/收盘时点编排协作者（0010 步骤 5）：14:30 日终估值（转 breaker）、
        # 15:05 日线级别轮次（1d/1w/1mon 完整 bar；转 market_data，经 on_bar 驱动）、
        # 15:05 收盘清扫（转 orders 回填/终态同步/兜底 cancel）。持有幂等日期标记。
        # on_bar/emit/set_bridge_offline 经回调注入（不反向 import LiveEngine）；
        # 引擎保留三个 _maybe_* 薄方法 + 同名可读写 property 兼容测试直连。
        self._closer = DailyCloser(
            self.ctx,
            self.market_data,
            self._breaker,
            self._order_machine,
            on_bar=self._handle_bar,
            # 日历以 provider 注入：测试在构造后替换 engine._calendar，需读到最新值。
            calendar_provider=lambda: self._calendar,
            emit=self._emit,
            set_bridge_offline=lambda: self._mark_bridge_offline(now_shanghai()),
            # 外部信号文件模式：日终 1w/1mon 通达信补注入同样停用（信号只来自文件）。
            external_signals_only=self._signal_source is not None,
            # 14:50 提前收盘价：仅 auto 模式生效（预警模式不下单，无意义）。
            advance_close_enabled=(
                self._advance_close_enabled_cfg and self.execution_mode == "auto"),
            advance_close_time=self._advance_close_time_cfg,
            snapshot_synth=self._snapshot_synth,
        )

        # 已分发过周期边界的 1m stime 集合——同根 bar 二次触发时挡掉重复周期分发。
        # BarPoller 按 code 独立判定完成：慢股票在下一轮 poll 才完成同一 stime
        # （真机 14:30 被二次驱动，15m 二次白拉）。周期分发全局重拉全 stock_codes +
        # 周期策略二次求值 = 白拉；1m 策略不受影响（每轮 bar.stocks 只含当轮新完成
        # 股票，慢股票仍在其完成那轮被驱动求值）。stime 含日期，跨日无碰撞。
        # 只在「成功派发」后才登记（新鲜度闸跳过不登记，留给 _pending_boundaries 补偿）。
        # 属 _on_bar 编排状态（非行情数据本身），留引擎。
        self._dispatched_boundaries: set = set()
        # 待补偿边界：{boundary_time: {period}}。周期边界因「最新完成 bar 非当日」
        # （gate=period_bar_freshness）整批跳过时登记；后续 1m bar 触发时重试，
        # 拿到当日 bar 派发成功即移除；超 _PENDING_BOUNDARY_TTL 放弃（记 WARNING）。
        self._pending_boundaries: Dict[datetime, Set[str]] = {}

        self._running = False
        # I-5：stop 请求标记（stop() 首行置 True、start() 重置 False）。提交链入口
        # 据此拒新单——_running 兼作「循环是否在跑」，直调 _handle_bar 的同步路径
        # （测试/ recover 前置）需要放行，故用独立标记表达「已请求停止」语义。
        self._stop_requested = False
        self._task: Optional[asyncio.Task] = None
        # G5：/deals 回填轮询的独立任务（与主循环并行，见 _deals_loop）
        self._deals_task: Optional[asyncio.Task] = None
        # 审计 #3：_loop 的同步阻塞 I/O（heartbeat/poll/_maybe_daily_*，全用同步
        # httpx.Client）整轮丢到此单 worker 线程执行——事件循环不再被阻塞 I/O 冻结。
        # max_workers=1 让主循环 tick 严格串行：signal_cache / 行情缓存等主池私有状态
        # 无并发竞争（沿用既有「同事件循环协作式」的串行语义，只是换到线程）。
        # 单 worker 还保证 cancel 后已排队的 tick 不与下一轮重叠（asyncio.Lock 在 cancel
        # 时会释放，无法保证；单 worker 天然排队）。
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=1)
        # I-8（audit）：_deals_loop 的 tick 投独立单 worker 池——与主循环各自的
        # 1-worker 池互不饿死（真机：主循环拉 ~73 只行情 50-57s/轮时，5s 节拍的
        # deals 回填不再排队等主 tick）。共享资源由 TQ 锁 get_tdx_lock 串行化；
        # I-8/C2（终审）：两池**真并发**后，跨池记账（pending_orders 增删遍历、
        # backfill/apply、持仓现金更新）不再靠协作式约定，由引擎级 _book_lock 串行化
        # （锁序恒为 book_lock → get_tdx_lock，无反向获取，无死锁）。
        self._deals_executor: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="live-deals"
        )
        # C2（终审）：引擎级记账锁。主池（bar/approve/submit/超时收割/收盘清扫）与
        # deals 池（5s 回填）独立后，三处真并发冲突点由调用点持锁串行：
        #   ① _tick_main 超时收割段（expire 兜底 + cancel_unfilled 收割 apply）
        #   ② 收盘清扫 apply 段（_maybe_close_sweep → DailyCloser）
        #   ③ _tick_deals 回填段（poll_deals → backfill → apply / pending 增删）
        #   ④ _handle_bar 遍历 _pending_orders.values()（vs 跨池 pop/整字典替换）
        # RLock：锁内允许调 SDK（get_tdx_lock 不反向取本锁），嵌套持锁安全。
        self._book_lock = threading.RLock()
        # P0-2：把引擎账本锁注入各组合。deals 回填池在另一线程插入新仓/镜像
        # quantity·avg_cost（同持 _book_lock），组合侧风控判定/额度计算/净值合计
        # 在锁内取值 → 不再出现「dictionary changed size during iteration」吞掉
        # 整批信号，止损数量也不会读到半更新持仓。回测装配不注入（book_lock=None）。
        for p in self.ctx.portfolios:
            p.book_lock = self._book_lock
        # _emit 跨线程投递用：start() 时捕获运行中的事件循环；worker 线程内 _emit 经
        # call_soon_threadsafe 回到事件线程 put_nowait（asyncio.Queue 非线程安全）。
        # None = 尚未 start（直接同步调 _emit 的旧测试路径 → 直接 put_nowait）。
        self._loop_ref: Optional[asyncio.AbstractEventLoop] = None
        self._bridge_online = True
        # P2-6：桥离线起点（只记第一次，供恢复时计算缺口与补派 count；恢复成功后清空）
        self._offline_since: Optional[datetime] = None
        # 恢复首轮的补派计划（count/gap_start）与待消费的放大 count（P2-6）
        self._gap_recovery_plan: Optional[dict] = None
        self._pending_backfill_count: int = 0
        # P1-3（2026-11-12）：补派 poll 抛非 BridgeUnavailableError 异常时计数已被
        # 消费（旧实现静默清零 = 永久放弃补派）。保留计数重试，并按缺口计划只落
        # 一条 `gate=bar_gap/action=retry` 事件（成功审计或新缺口计划时复位）。
        self._backfill_retry_reported: bool = False
        # P2-8：上一轮各段耗时（ms）与最近一次饱和告警时间（工作台/日志可观测）
        self._last_tick_profile: Dict[str, int] = {}
        self._tick_profile_total_ms: int = 0
        self._tick_saturation_warned_at: Optional[datetime] = None
        # 掘金「桥 1m 放行探测」辅助状态（默认行情源仍是快照合成，桥只辅助）：
        # 本会话已借桥 bar 放行过的 code（不再重复探）+ 失败退避截止时间。
        self._bridge_probe_released_codes: set = set()
        self._bridge_probe_next_at: Optional[datetime] = None
        # 继承仓位市值扣减虚拟现金后若现金为负（资金基准填得小于继承市值），
        # 只告警一次，避免 60s 快照节拍刷屏。
        self._neg_cash_warned = False
        # 存活监控信号（进程内 watchdog 读）：主循环每轮起始刷新心跳；start 记录
        # 启动时刻用于预热宽限。仅两个赋值，不增加任何 TQ/桥调用。
        self._started_at: Optional[datetime] = None
        self._last_tick_at: Optional[datetime] = None
        # 三个幂等日期标记已迁入 self._closer（DailyCloser，0010 步骤 5）；下列赋值经
        # 同名 property 委托穿透到 closer 字段（初始化为 None，与 closer 默认一致）。
        self._last_daily_date = None
        self._last_daily_bar_date = None
        self._last_close_sweep_date = None
        self._last_advance_close_date = None
        # _pending_orders / _last_backfill_time 已迁入 self._order_machine（0010 步骤 3）；
        # 引擎保留同名可读写 property 委托（recover 直接赋值 self._pending_orders = {...}、
        # 测试直连 engine._pending_orders 穿透到同一份字典）。
        # F5：最近处理 bar 的强引用（多组合共享同一 bar 对象 → 每 bar 只刷一次桥可用
        # 持仓；强引用防对象 id 复用导致的漏刷）。None=尚未处理任何 bar。
        self._available_bar: Optional["BarEvent"] = None
        # P1（2026-11-09）：最近一次收到的分钟级 bar 的交易日，以及「上一根之前的
        # 值」（bar 日期推进时才更新，多组合共享同一 bar 时判定一致）。用于 deferred
        # 过期判定区分「due 当日行情失效、根本没机会放行」与「真跨过放行窗口」。
        self._last_minute_bar_date: Optional[date] = None
        self._prev_minute_bar_date: Optional[date] = None
        # D4/H4：每组合已持久化的熔断计数（LiveSessionPortfolio.circuit_breaker_count）
        # 已迁入 self._breaker（BreakerService，0010 步骤 4）；引擎保留同名可读写
        # property 委托（recover 直接赋值 self._breaker_count_written[pid]=n、测试直连
        # engine._breaker_count_written 穿透到同一份字典）。
        # D3：最近一次对账结果——虚拟持仓 vs 真实持仓按 code 比对的差异列表
        # （{code, virtual, real, diff, computed_at}）。只记录/告警，不自动修正账面
        # （首期安全）；recover/收盘复查/每轮账户采集导入后都会重算（见
        # _set_reconcile_mismatches）。
        self._reconcile_mismatches: List[dict] = []
        # I-7：收盘复查对账的幂等日期标记（每交易日一次，见 _maybe_reconcile_after_close）
        self._last_reconcile_date: Optional[date] = None
        # 人工干预：柜台持仓里「平台台账 + 在途单」解释不了的增减量的已报去重键
        # （code, virtual, real, action），防同一差异每轮采集重复刷审计/SSE。
        self._manual_intervention_reported: set = set()
        # 人工干预：柜台持仓行连续缺失计数（code -> 次数）。连续
        # _COUNTER_ABSENT_CONFIRM 次确认才按「柜台已清仓」清理虚拟账本，
        # 防单次行情/持仓读取不完整误清仓。
        self._counter_absent_streak: Dict[str, int] = {}
        # P1-8：零估值仓（三级兜底全缺 → 市值按 0）risk 事件去重（每 code 每日一次）
        self._valuation_gap_date: Optional[date] = None
        self._valuation_gap_noted: set = set()
        # B5：SSE 事件总线（0010 步骤 1 抽到 core.engine.live.event_bus.EventBus）。
        # 持有订阅队列集合、跨线程投递、ping 心跳；引擎保留 _emit/stream_events 薄委托，
        # 并暴露 _stream_subscribers/_stream_ping_interval 兼容测试直连。
        self._event_bus = EventBus(ping_interval=stream_ping_interval, clock=now_shanghai)

    @property
    def _stream_subscribers(self) -> List["asyncio.Queue"]:
        """兼容测试直连（append/extend/in）：转发到 EventBus 的订阅列表。"""
        return self._event_bus.subscribers

    @property
    def _stream_ping_interval(self) -> float:
        return self._event_bus.ping_interval

    @_stream_ping_interval.setter
    def _stream_ping_interval(self, value: float) -> None:
        self._event_bus.ping_interval = value

    # ---- EngineContext 委托（0010 步骤 2a）----
    # 这些字段已迁入 self.ctx；保留同名 property 让既有调用点与测试的直接赋值
    # （engine.portfolios=[...]、engine._code_period_count={...}、engine._dispatcher=...）
    # 穿透到同一份 ctx 对象，纯搬移不改行为。
    @property
    def session_id(self) -> int:
        return self.ctx.session_id

    @session_id.setter
    def session_id(self, value: int) -> None:
        self.ctx.session_id = value

    @property
    def portfolios(self) -> List[Portfolio]:
        return self.ctx.portfolios

    @portfolios.setter
    def portfolios(self, value: List[Portfolio]) -> None:
        self.ctx.portfolios = value
        # 构造后替换组合列表（测试直连 / recover 重建）也要注入决策采集器与账本锁
        rec = getattr(self, "_recorder", None)
        if rec is not None:
            for p in value:
                p.set_recorder(rec)
        lock = getattr(self, "_book_lock", None)
        if lock is not None:
            for p in value:
                p.book_lock = lock

    @property
    def _dispatcher(self) -> HttpBridgeDispatcher:
        return self.ctx.dispatcher

    @_dispatcher.setter
    def _dispatcher(self, value: HttpBridgeDispatcher) -> None:
        self.ctx.dispatcher = value

    @property
    def _db_session_factory(self) -> Callable[[], Session]:
        return self.ctx.db_session_factory

    @_db_session_factory.setter
    def _db_session_factory(self, value: Callable[[], Session]) -> None:
        self.ctx.db_session_factory = value

    @property
    def _code_period_count(self) -> Dict[tuple, int]:
        return self.ctx.code_period_count

    @_code_period_count.setter
    def _code_period_count(self, value: Dict[tuple, int]) -> None:
        self.ctx.code_period_count = value

    # ---- MarketDataService 委托（0010 步骤 2b）----
    # 缓存/公式注入配置归 MarketDataService；保留可读写 property 让既有测试直连
    # （engine.signal_cache={...}、engine._tq_formula.compute_injected_batch=...、
    # engine._preheat_cache[...] = ...）穿透到同一份服务对象。
    @property
    def signal_cache(self) -> Dict:
        return self.market_data.signal_cache

    @signal_cache.setter
    def signal_cache(self, value: Dict) -> None:
        self.market_data.signal_cache = value

    @property
    def _preheat_cache(self) -> Dict[tuple, dict]:
        return self.market_data._preheat_cache

    @property
    def _tq_formula(self):
        return self.market_data._tq_formula

    @property
    def _formula_by_strategy(self) -> Dict[int, str]:
        return self.market_data._formula_by_strategy

    @property
    def _formula_count(self) -> int:
        return self.market_data._formula_count

    @property
    def _formula_count_by_name(self) -> Dict[str, int]:
        return self.market_data._formula_count_by_name

    @property
    def _period_count(self) -> Dict[str, int]:
        return self.market_data._period_count

    @property
    def _strategy_periods(self) -> set:
        return self.market_data._strategy_periods

    # ---- OrderStateMachine 委托（0010 步骤 3）----
    # 在途单字典/最近回填时点归 OrderStateMachine；保留可读写 property 让 recover 的
    # 直接赋值（self._pending_orders = {...}）与测试直连穿透到同一份对象。
    @property
    def _pending_orders(self) -> Dict[int, "LiveOrder"]:
        return self._order_machine.pending_orders

    @_pending_orders.setter
    def _pending_orders(self, value: Dict[int, "LiveOrder"]) -> None:
        self._order_machine.pending_orders = value

    @property
    def _last_backfill_time(self) -> Optional[datetime]:
        return self._order_machine.last_backfill_time

    @_last_backfill_time.setter
    def _last_backfill_time(self, value: Optional[datetime]) -> None:
        self._order_machine.last_backfill_time = value

    # _breaker_count_written 归 BreakerService（0010 步骤 4）；返回活字典引用，
    # 让 recover 的 self._breaker_count_written[k]=n 与测试的 engine._breaker_count_written[k]=n
    # 穿透到同一份对象。
    @property
    def _breaker_count_written(self) -> Dict[int, int]:
        return self._breaker.counts_written

    @_breaker_count_written.setter
    def _breaker_count_written(self, value: Dict[int, int]) -> None:
        self._breaker.counts_written = value

    # ---- DailyCloser 委托（0010 步骤 5）----
    # 三个幂等日期标记归 DailyCloser；保留可读写 property 让测试直连
    # （engine._last_daily_date = today、engine._last_close_sweep_date 读判等）穿透。
    @property
    def _last_daily_date(self):
        return self._closer.last_daily_date

    @_last_daily_date.setter
    def _last_daily_date(self, value) -> None:
        self._closer.last_daily_date = value

    @property
    def _last_daily_bar_date(self):
        return self._closer.last_daily_bar_date

    @_last_daily_bar_date.setter
    def _last_daily_bar_date(self, value) -> None:
        self._closer.last_daily_bar_date = value

    @property
    def _last_close_sweep_date(self):
        return self._closer.last_close_sweep_date

    @_last_close_sweep_date.setter
    def _last_close_sweep_date(self, value) -> None:
        self._closer.last_close_sweep_date = value

    @property
    def _last_advance_close_date(self):
        return self._closer.last_advance_close_date

    @_last_advance_close_date.setter
    def _last_advance_close_date(self, value) -> None:
        self._closer.last_advance_close_date = value

    # ---------------- B5 SSE 事件流（委托 EventBus，0010 步骤 1）----------------
    def _first_emit_for_key(self, key: Optional[tuple]) -> bool:
        """同键事件只放行一次（key=None 不去重，用于无 bar_time 的事件）。

        仅用于展示层收敛（decision/signal 落库与 SSE），不参与任何交易闸门。
        """
        if key is None:
            return True
        if key in self._emitted_event_keys:
            return False
        self._emitted_event_keys[key] = None
        while len(self._emitted_event_keys) > _EVENT_DEDUPE_MAX:
            self._emitted_event_keys.popitem(last=False)
        return True

    def _emit(self, event_type: str, payload: dict) -> None:
        """向所有 SSE 订阅队列广播（委托 EventBus.emit，线程安全语义见 event_bus）。"""
        self._emit_file_handoff(event_type, payload)
        self._event_bus.emit(event_type, payload)

    def _emit_file_handoff(self, event_type: str, payload: dict) -> None:
        """挂了执行契约文件交接层的会话：signal/decision/risk 事件同步写交接文件。

        失败只 warning 不影响主流程（SSE 投递与落库是权威通道，交接文件是
        外部诊断/对账副本）。order 事件不落（orders.jsonl 由 dispatcher 专职写）。
        """
        handoff = self._file_handoff
        if handoff is None or event_type not in ("signal", "decision", "risk"):
            return
        try:
            entry = {"type": event_type, **payload}
            if event_type == "signal":
                handoff.write_signal(entry)
            else:
                handoff.write_risk(entry)
        except Exception:  # noqa: BLE001
            logger.warning("file handoff write failed (%s)", event_type, exc_info=True)

    def _emit_to_subscribers(self, ev: dict) -> None:
        """兼容旧调用/测试：直接向订阅队列投递（委托 EventBus）。"""
        self._event_bus._emit_to_subscribers(ev)

    def stream_events(self) -> AsyncIterator[dict]:
        """SSE 事件流（委托 EventBus.stream；公共签名不变，live.py 与测试仍调此方法）。

        直接返回内部 async generator（不用 `async for ... yield` 包裹），这样调用方
        aclose() 直接作用于 bus 的生成器，其 finally（退订队列）立即执行。
        """
        return self._event_bus.stream(lambda: self._running)

    # ---------------- 行情/信号缓存（委托 MarketDataService，0010 步骤 2b）----------------
    # 以下方法/静态方法保留为薄委托：实现已迁到 core.engine.live.market_data.MarketDataService，
    # 缓存（_preheat_cache/signal_cache）归服务所有。保留同名入口让既有调用点与测试直连
    # （engine._preheat()、engine._get_bars_with_increment(...)、LiveEngine._bars_to_formula_df(...)
    # 等）无需改动，纯搬移不改行为。
    def _preheat(self, only_missing: bool = False) -> None:
        self.market_data.preheat(only_missing=only_missing)

    def _make_cache_entry(self, bars: list, count: int) -> dict:
        return self.market_data._make_cache_entry(bars, count)

    def _get_bars_with_increment(self, code: str, period: str, count: int) -> list:
        return self.market_data.get_bars_with_increment(code, period, count)

    @staticmethod
    def _bar_stime(bar: dict) -> Optional[datetime]:
        return MarketDataService._bar_stime(bar)

    @staticmethod
    def _sort_and_cap(bars: list, count: int) -> list:
        return MarketDataService._sort_and_cap(bars, count)

    @staticmethod
    def _max_stime(bars: list) -> Optional[datetime]:
        return MarketDataService._max_stime(bars)

    # ---------------- 生命周期 ----------------
    async def start(self) -> None:
        """起 asyncio 循环任务，绑定 BarPoller.on_bar 回调。"""
        if self._running:
            return
        self._running = True
        self._started_at = now_shanghai()
        self._last_tick_at = self._started_at
        self._stop_requested = False  # 重启支持：清掉上次 stop 的拒单闸
        # 审计 #3：捕获运行中的事件循环，供 _emit 跨线程 call_soon_threadsafe 回投。
        self._loop_ref = asyncio.get_running_loop()
        self._event_bus.bind_loop(self._loop_ref)
        # 审计 #3：stop() 会 shutdown executor（释放非 daemon worker 线程，防进程退出
        # 挂起）；若引擎被重启（同实例二次 start），旧 executor 已 shutdown → 重建一个。
        # 每次 start 重建开销可忽略（1 线程，session 生命周期内仅一次）。
        if self._executor is not None:
            try:
                self._executor.shutdown(wait=False)
            except Exception:  # noqa: BLE001
                pass
        self._executor = ThreadPoolExecutor(max_workers=1)
        # I-8：deals 独立池同理随 start 重建（stop 已 shutdown 旧池）
        if self._deals_executor is not None:
            try:
                self._deals_executor.shutdown(wait=False)
            except Exception:  # noqa: BLE001
                pass
        self._deals_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="live-deals"
        )
        # I-6（audit）：启动一次性同步重活（通达信 1w/1mon 注入 + 全量预热拉取）
        # 经 executor 执行——事件循环不被 TDX 计算/桥拉取阻塞（SSE/HTTP 保持响应）。
        # await 等其完成再起循环，时序语义与原同步内联一致。
        await self._loop_ref.run_in_executor(self._executor, self._sync_startup)
        # 通道价格模式声明（盘前强制 approval 时记一条决策事件，工作台可见）
        self._announce_pre_open_price_mode()
        self._bar_poller.on_bar = self._on_bar
        # 基线那根（首次建基线/离线恢复首根）也交付引擎：只放行 deferred，不跑公式
        self._bar_poller.on_baseline_bar = self._on_bar
        self._task = asyncio.create_task(self._loop())
        self._task.add_done_callback(functools.partial(self._on_loop_done, "main"))
        # G5：独立 /deals 回填轮询（5s 节拍，不随主循环 60s 拉 bar 一起）
        if self.execution_mode != "alert":
            self._deals_task = asyncio.create_task(self._deals_loop())
            self._deals_task.add_done_callback(functools.partial(self._on_loop_done, "deals"))

    def _on_loop_done(self, name: str, task: asyncio.Task) -> None:
        """循环任务死亡监控（防 2026-11-03 08:54 式静默僵死）。

        生产实证（2026-11-03）：``_loop`` 任务被 CancelledError 静默杀死
        （非 stop 路径——stop 先置 ``_running=False`` 再 cancel），引擎从此
        零 tick：当日 deferred 单永不放行、盘中风控停摆，且无任何日志。
        本回调分类处置：

        - cancelled 且 ``_running=False`` → 正常 stop，静默；
        - cancelled 且 ``_running=True`` → 异常死亡 → WARNING + 自愈重启；
        - 异常退出（``task.exception()`` 非 None）→ CRITICAL + 自愈重启；
        - 无异常退出（while 条件意外转假）→ WARNING + 自愈重启。

        注：SystemExit/KeyboardInterrupt 在 tick 内逃逸会直接杀死整个事件
        循环（CPython Task.__step 对 BaseException 子类 set_exception 后
        re-raise 进 loop.run_forever）——那是进程级崩溃（uvicorn 退出，必然
        可见），不属于本监控的静默僵尸范畴。done-callback 在事件循环线程
        执行，create_task 安全；name 由 partial 按任务绑定（main/deals
        双循环并发死亡不串标）。
        """
        if task.cancelled():
            if not self._running:
                return
            logger.warning(
                "%s loop task cancelled unexpectedly while running "
                "(session %s) — auto-restart",
                name, self.session_id,
            )
        else:
            exc = task.exception()
            if exc is not None:
                logger.critical(
                    "%s loop task died with %r (session %s) — auto-restart",
                    name, exc, self.session_id,
                )
            else:
                logger.warning(
                    "%s loop exited unexpectedly (session %s) — auto-restart",
                    name, self.session_id,
                )
        if not self._running:
            return
        coro = self._loop if name == "main" else self._deals_loop
        new_task = asyncio.create_task(coro())
        new_task.add_done_callback(functools.partial(self._on_loop_done, name))
        if name == "main":
            self._task = new_task
        else:
            self._deals_task = new_task

    def _sync_startup(self) -> None:
        """start() 的一次性同步重活（I-6：在主 executor worker 线程执行）。

        C6(C)：启动时通达信注入 1w/1mon 策略信号（桥端 xtdata 拉不到，仅此通路），
        一次同步 TDX 计算（get_tdx_lock 串行）；随后预热 1m/5m/15m/30m/1h 历史 bar
        到 _preheat_cache（运行期增量拼接，省每 bar/每边界全量重拉）。必须在 _loop
        起来前完成（否则首个 bar 触发时缓存未就绪）；单 (code,period) 失败不阻断，
        运行期自愈。
        """
        # 外部信号文件模式：1w/1mon 的 TQ 启动注入同样停用（信号只来自预警文件，
        # 避免启动即有 TQ 预填信号与文件路径并存）。预热保留（行情/风控仍需历史）。
        if self.execution_mode != "alert" and self._signal_source is None:
            self._inject_startup_periods(
                datetime.combine(now_shanghai().date(), datetime.min.time())
            )
        self._preheat()

    def _dispatcher_close_safe(self) -> None:
        """TQ 1.3.1：释放 dispatcher 资源（best-effort，可安全重复调用）。

        stop() 经主 executor 执行本方法（I-5）：close 排队于在途 tick 之后——
        在途 tick 持 get_tdx_lock 跑完后 close 自然获得锁，事件循环不被 TDX
        退订对账阻塞。TdxTqDispatcher.close 做行情退订对账；其余通道 close 为
        轻量/无操作；QMT 桥无 close 则跳过。失败仅告警，不阻断停止流程。
        """
        close = getattr(self._dispatcher, "close", None)
        if callable(close):
            try:
                close()
            except Exception:  # noqa: BLE001
                logger.warning("dispatcher close failed on stop", exc_info=True)

    def _cancel_inflight_orders_on_stop(self) -> None:
        """I-3（终审）：stop 时对在途真实单 best-effort 撤单（主 executor worker 执行）。

        在途集合以 DB 为准（submitted/partial，与 poll_deals/sync_pending_orders 同一
        口径；_pending_orders 缓存的 ORM 实例可能已随原 session 关闭而 detached）：
        有 order_ref → 尽力 ``dispatcher.cancel_order(ref, code)``（复用 A-1 撤单；
        异常吞并计数）；通道不支持撤单（如 QMT 桥）或单无 order_ref → 计入风险汇总。
        停止后不再有轮询收敛这些单，撤单失败/无 ref 的部分汇成**一条**
        risk 事件（中文，工作台事件日志可见），提示人工核对柜台委托。
        """
        factory = self._db_session_factory
        if factory is None:
            return
        db = factory()
        if db is None:
            # alert 模式（无 DB、不下单）：无在途单可撤
            return
        try:
            inflight = (
                db.query(LiveOrder)
                .filter(
                    LiveOrder.live_session_id == self.session_id,
                    LiveOrder.status.in_(["submitted", "partial"]),
                )
                .all()
            )
            if not inflight:
                return
            logger.info(
                "stop: %d in-flight order(s) at stop, best-effort cancel (session %s)",
                len(inflight), self.session_id,
            )
            cancel = getattr(self._dispatcher, "cancel_order", None)
            supports_cancel = (
                callable(cancel)
                and getattr(self._dispatcher, "supports_cancel", True) is not False
            )
            failed: List[str] = []
            no_ref: List[str] = []
            for lo in inflight:
                label = "%s %s" % (lo.stock_code, (lo.trade_type or "").upper())
                ref = lo.order_ref
                if not ref:
                    no_ref.append(label)
                    continue
                if not supports_cancel:
                    failed.append("%s（%s，通道不支持撤单）" % (ref, label))
                    continue
                try:
                    if cancel(ref, lo.stock_code):
                        logger.info(
                            "stop: cancel requested for in-flight order %s (ref=%s)",
                            lo.id, ref,
                        )
                    else:
                        failed.append("%s（%s，撤单返回失败：%s）" % (
                            ref, label,
                            getattr(self._dispatcher, "last_error", "") or "无原因"))
                except Exception as e:  # noqa: BLE001 — best-effort：单笔失败不阻断停止
                    failed.append("%s（%s，撤单异常：%s）" % (ref, label, e))
                    logger.warning(
                        "stop: cancel_order error for order %s ref %s: %s", lo.id, ref, e,
                    )
            if not failed and not no_ref:
                return
            parts = []
            if failed:
                parts.append("撤单失败 %d 笔：%s" % (len(failed), "；".join(failed)))
            if no_ref:
                parts.append(
                    "无 order_ref 在途单 %d 笔（%s），停止后无法追踪终态"
                    % (len(no_ref), "、".join(no_ref))
                )
            message = (
                "会话停止时存在未完结在途单——%s，请人工核对柜台委托与成交" % "；".join(parts)
            )
            self._emit("risk", {"rule": "stop_inflight_orders", "message": message})
            logger.warning("stop: %s (session %s)", message, self.session_id)
        finally:
            db.close()

    async def stop(self) -> None:
        """停循环任务。"""
        self._running = False
        # I-5：先置拒单闸——cancel 期间在途 tick 若再走 _handle_bar 不发新单
        self._stop_requested = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                logger.exception("live loop task ended with error on stop")
            self._task = None
        if self._deals_task is not None:
            self._deals_task.cancel()
            try:
                await self._deals_task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                logger.exception("deals loop task ended with error on stop")
            self._deals_task = None
        # I-3（终审）：置闸后、close 前对在途真实单 best-effort 撤单。此刻两循环任务
        # 已 cancel，在途 tick 排在主单 worker 池里先跑完，本步经同一池串行执行——
        # 读到的 _pending_orders 不再被并发修改。撤单走 worker 线程（SDK/HTTP 同步调），
        # risk 汇总事件经 EventBus 跨线程回投（loop 引用尚未清空）。
        try:
            await asyncio.get_running_loop().run_in_executor(
                self._executor, self._cancel_inflight_orders_on_stop
            )
        except RuntimeError:
            self._cancel_inflight_orders_on_stop()
        # 审计 #3：停止后清空 loop 引用——worker 线程已不再产生 _emit；
        # 之后若再有同步直调 _emit（测试 / 外部），走「无 loop」直接 put_nowait 路径。
        self._loop_ref = None
        self._event_bus.clear_loop()
        # B-1：停会话前把内存 deferred 队列**如实重落库**——旧实现只清空内存并
        # 无条件打「kept in DB」，但 recover 转移语义下 DB 行早已被删，停会话即丢
        # （2026-11-09 实证：14:59 restart 后 4 单仅剩内存）。此处兜底：无行单补插
        # status=deferred 行、已有行不动，保证任何时刻重启都能从 DB 重建。
        if self._signal_bar_deferred:
            attempted = len(self._signal_bar_deferred)
            try:
                kept = await asyncio.get_running_loop().run_in_executor(
                    self._executor, self._persist_deferred_queue_on_stop
                )
            except RuntimeError:
                # executor 已 shutdown（重复 stop）→ 同步兜底
                kept = self._persist_deferred_queue_on_stop()
            logger.info(
                "stop: %d deferred daily-signal order(s) persisted as "
                "status=deferred (rows kept/created: %d), next session recover "
                "will restore (session %s)",
                attempted, kept, self.session_id,
            )
        self._signal_bar_deferred.clear()
        # 批次2：清空拆单队列。在途子单（已落库 submitted）由 I-3 stop 撤单与
        # 超时撤单兜底；未提交子单不落库，随队列丢弃（预期行为，记日志留痕）。
        if self._split_children:
            dropped = sum(
                len(e.get("pending") or [])
                for e in self._split_children.values()
            )
            logger.info(
                "stop: drop %d pending split child order(s) across %d queue(s) "
                "(session %s)", dropped, len(self._split_children), self.session_id,
            )
        self._split_children.clear()
        # I-5：dispatcher.close 经同一主 executor 执行——在途 tick 持 get_tdx_lock
        # 跑完后 close 自然获得锁（同池 max_workers=1 串行排队），事件循环不被
        # TDX 退订对账阻塞（原先在事件循环线程同步 close，秒级冻结）。
        # RuntimeError = executor 已 shutdown（重复 stop）→ 同步兜底（close 幂等）。
        try:
            await asyncio.get_running_loop().run_in_executor(
                self._executor, self._dispatcher_close_safe
            )
        except RuntimeError:
            self._dispatcher_close_safe()
        # I-8：两池都 shutdown（wait=False：close 已在上方 await 完成，在途 tick
        # 是 loopback HTTP/本地计算，瞬间完成；任务已 cancel 不会再提交新 tick）。
        # 释放非 daemon worker 线程，防进程退出挂起。
        if self._executor is not None:
            self._executor.shutdown(wait=False)
        if self._deals_executor is not None:
            self._deals_executor.shutdown(wait=False)

    async def _loop(self) -> None:
        """主循环：心跳 → 拉 bar → 日终 → sleep。桥离线则暂停下单、标状态，不抛异常。

        /deals 成交回报回填不在本循环（G5）：独立 _deals_loop 5s 节拍处理，
        免得成交秒级回报被 60s 拉 bar 节拍拖慢。

        审计 #3：同步阻塞 I/O（heartbeat/poll/_maybe_daily_*，全用同步 httpx.Client）
        整轮丢到单 worker 线程池执行（_tick_main），事件循环只负责调度 + sleep——
        不再被 60s 拉 bar / 桥超时阻塞，HTTP 请求与 SSE 流不再冻结。
        """
        while self._running:
            started = time.monotonic()
            try:
                await asyncio.get_running_loop().run_in_executor(
                    self._executor, self._tick_main
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                # _tick_main 内部已兜底各类异常并记日志；此处的 executor 层异常兜底防漏
                logger.exception("live loop executor error")
            # P2-8：按「轮次起点对齐」睡眠——旧实现每轮固定再睡满 poll_interval，
            # 实际节拍 = 轮次耗时 + 60s（真机 50-57s 拉行情 → 约 110s 一轮），
            # 1m bar 信号延迟被放大近一倍且行情一抖就顺延。现在补足剩余时间，
            # 让轮次起点稳定在 ~60s；轮次本身超时（>interval）时只留 1s 让出，
            # 下一轮立即开始（超时告警由 _record_tick_profile 记事件）。
            interval = max(0.0, float(self._poll_interval or 0))
            if interval <= 0:
                delay = 0.0
            else:
                delay = interval - (time.monotonic() - started)
                if delay < 0:
                    # 轮次已超时：生产节拍（≥10s）留 1s 让出；测试/极短节拍
                    # （0.01s 级）不额外减速，保持原「连续轮」语义
                    delay = min(1.0, interval) if interval >= 10 else 0.0
            await asyncio.sleep(delay)

    # ---------------- 行情轮询窗口（收盘后停拉行情）----------------
    def _quote_poll_active(self, now: datetime) -> bool:
        """now 时刻是否允许行情轮询：交易日 + 轮询窗口内。

        enabled=False（显式关窗）→ 永远允许（等价旧行为：全天轮询，非交易日也轮）。
        """
        window = self._quote_polling
        if not window.enabled:
            return True
        return self._calendar.is_trading_day(now.date()) and window.is_open(now)

    def _enter_quote_poll_pause(self, now: datetime) -> None:
        """进入轮询暂停：只在状态跳变时调用一次（不按 tick 刷屏）。"""
        if not self._calendar.is_trading_day(now.date()):
            reason = "非交易日"
        else:
            reason = "已过收盘轮询截止 %s" % self._quote_polling.pause_time.strftime("%H:%M")
        next_at = self._quote_polling.next_resume_at(now, self._calendar.is_trading_day)
        next_text = next_at.strftime("%Y-%m-%d %H:%M") if next_at else "下一交易日"
        logger.info(
            "行情轮询已暂停（会话 %s）：%s，下次恢复 %s；账户快照/成交回填/收盘清扫照常",
            self.session_id,
            reason,
            next_text,
        )
        # 状态跳变同步发 risk 事件（SSE 可见）；调用方以 _quote_poll_paused 保证只发一次。
        self._emit("risk", {
            "rule": "quote_polling_paused",
            "session_id": self.session_id,
            "engine": self._engine_key,
            "reason": reason,
            "next_resume_at": next_at.strftime("%Y-%m-%d %H:%M") if next_at else None,
            "message": "行情轮询已暂停：%s，下次恢复 %s；快照/成交回填/收盘清扫照常"
                       % (reason, next_text),
        })

    def _leave_quote_poll_pause(self, now: datetime) -> None:
        """退出轮询暂停（恢复）：打边界守护 + 预热（仅补缺失缓存，不重拉历史）。"""
        if hasattr(self._bar_poller, "mark_resume_boundary"):
            # 无水位的新代码（暂停期扩池）只建基线；恢复当日之前的 bar 一律不触发
            self._bar_poller.mark_resume_boundary(now.date())
        started = time.perf_counter()
        try:
            self._preheat(only_missing=True)
        except Exception:  # noqa: BLE001
            logger.exception("恢复行情轮询预热失败（会话 %s）", self.session_id)
        elapsed = time.perf_counter() - started
        logger.info(
            "行情轮询已恢复（会话 %s，%s）：预热 %.1fs（仅补缺失缓存，不重拉历史）",
            self.session_id, now.strftime("%H:%M:%S"), elapsed,
        )
        # 状态跳变同步发 risk 事件（SSE 可见）；调用方以 _quote_poll_paused 保证只发一次。
        self._emit("risk", {
            "rule": "quote_polling_resumed",
            "session_id": self.session_id,
            "engine": self._engine_key,
            "resumed_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "preheat_seconds": round(elapsed, 1),
            "message": "行情轮询已恢复：进入 %s 窗口，已预热缺失行情"
                       % self._quote_polling.resume_time.strftime("%H:%M"),
        })

    def _run_daily_schedule(self) -> None:
        """日终估值 / 15:05 日线级别轮次 / B 提前收盘价轮次（心跳门控之外）。

        P1（2026-11-09）：这三段只依赖本地快照、TQ 本地日线与 DB，不需要桥在线；
        原先挂在心跳成功之后，桥一离线（QMT/掘金桥断）当天就整段跳过，1d/1w/1mon
        信号与 daily_loss/熔断推进静默丢失一整天。现在照常执行，逐段兜异常：
        某段取数时桥刚断（BridgeUnavailableError）只跳过该段，不影响其余段与下轮。
        """
        for name, fn in (
            ("daily_close", self._maybe_daily_close),
            ("daily_bars", self._maybe_daily_bars),
            ("advance_close", self._maybe_advance_daily_close),
        ):
            try:
                fn()
            except BridgeUnavailableError as e:
                self._mark_bridge_offline(now_shanghai())
                logger.warning("%s skipped: bridge unavailable: %s (session %s)",
                               name, e, self.session_id)
            except Exception:  # noqa: BLE001 — 单段失败不阻断其余段与下一轮
                logger.exception("%s error (session %s)", name, self.session_id)

    # ---------------- P2-6 桥离线缺口恢复 ----------------
    def _mark_bridge_offline(self, now: datetime) -> None:
        """置离线状态并只记录**首次**离线时点（缺口审计/补派起点）。"""
        self._bridge_online = False
        if self._offline_since is None:
            self._offline_since = now

    def _plan_gap_recovery(self, now: datetime) -> Optional[dict]:
        """离线恢复时决定「补派 / 放弃 + 审计」的处置计划（P2-6）。

        - 行情源支持历史回补（QMT/掘金桥 /quote）且缺口在同一天、轮询窗口内 →
          ``action=replay``，按缺口墙钟分钟数放大 count（封顶 240 根）；
        - 快照合成源（tdx/ths/alert/goldminer）→ ``unrecoverable``：断线期的 bar
          客观不存在，只能记缺口起止 + 缺口根数，不能静默；
        - 跨日 / 已过轮询窗口 → ``skip``：不补派（跨日旧 bar 补派 = 用昨日价格
          驱动今日下单），同样记审计事件。
        """
        gap_start = self._bar_poller.last_completed_stime or self._offline_since
        if gap_start is None:
            return None
        # P1-2：gap_end 取「最近一根已闭合分钟」（now 所在分钟正在形成，不能算进去，
        # 否则审计每轮必缺 1 根 → action 恒 partial）。
        plan = {"gap_start": gap_start,
                "gap_end": (now - timedelta(seconds=1)).replace(
                    second=0, microsecond=0)}
        if not getattr(self._bar_poller, "supports_backfill", False):
            plan.update(action="unrecoverable", reason="snapshot_source")
            return plan
        if gap_start.date() != now.date():
            plan.update(action="skip", reason="cross_day")
            return plan
        if not self._quote_poll_active(now):
            plan.update(action="skip", reason="quote_window_closed")
            return plan
        span_min = max(1, int((now - gap_start).total_seconds() // 60))
        count = min(_GAP_BACKFILL_MAX_BARS, max(_GAP_BACKFILL_MIN_BARS, span_min + 5))
        plan.update(action="replay", reason="bridge_recovered", count=count)
        return plan

    def _recover_after_offline(self, now: datetime) -> None:
        """离线→在线转场：按处置计划补派或重建基线，并落缺口审计（P2-6）。"""
        plan = self._plan_gap_recovery(now)
        if plan is not None and plan.get("count"):
            self._pending_backfill_count = int(plan["count"])
            self._gap_recovery_plan = plan
            # P1-3：新缺口计划 = 新的补派周期，失败告警可再落一条
            self._backfill_retry_reported = False
            logger.info(
                "[bar-gap] bridge back online: backfill %d bars (%s → %s) (session %s)",
                plan["count"], plan["gap_start"], now, self.session_id,
            )
        else:
            # 不可补派：沿用旧行为重建基线（离线期 bar 不触发），但必须留痕
            self._bar_poller.reset_baseline()
            if plan is not None:
                expected = _expected_minute_labels(
                    plan["gap_start"], plan["gap_end"])
                self._emit_gap_event(
                    plan, plan.get("action", "unrecoverable"),
                    expected=len(expected), replayed=0,
                    missing=expected)
        # 预热缓存 last_stime 已过期，增量拉 10 根可能接不上（断档超 10 根）：
        # 清空后下次 _get_bars_with_increment 走"缓存未命中"全量拉重建。
        self._preheat_cache.clear()
        # 快照合成器同步重置：丢弃断线期 forming 态与差分基线
        if self._snapshot_synth is not None:
            self._snapshot_synth.reset()
        # 桥恢复 → 清退避，下一轮即可再试放行探测
        self._bridge_probe_next_at = None
        self._offline_since = None

    def _audit_gap_recovery(self, now: datetime,
                            triggered: List[BarEvent]) -> None:
        """补派后记录实际覆盖情况：完整 / 部分（桥 count 上限截断）（P2-6）。"""
        plan = self._gap_recovery_plan
        self._gap_recovery_plan = None
        # P1-3：本轮补派已落地（审计已写），失败告警标志复位
        self._backfill_retry_reported = False
        if not plan:
            return
        expected = _expected_minute_labels(plan["gap_start"], plan["gap_end"])
        replayed_times = {b.bar_time for b in triggered}
        missing = [t for t in expected if t not in replayed_times]
        replayed = len(expected) - len(missing)
        self._emit_gap_event(
            plan, "replay" if not missing else "partial",
            expected=len(expected), replayed=replayed, missing=missing)

    def _emit_gap_event(self, plan: dict, action: str, *, expected: int,
                        replayed: int, missing: List[datetime]) -> None:
        """缺口审计事件（gate=bar_gap）：起止时间、缺口/补派根数、缺口首末。

        只写日志与 SSE 风险事件，不参与任何交易判断、不改账本。
        """
        gap_start = plan.get("gap_start")
        gap_end = plan.get("gap_end")
        reason = plan.get("reason") or ""
        payload = {
            "session_id": self.session_id,
            "engine": self._engine_key,
            "gate": "bar_gap",
            "action": action,
            "rule": reason or action,
            "gap_start": gap_start.isoformat() if gap_start else None,
            "gap_end": gap_end.isoformat() if gap_end else None,
            "gap_bars": expected,
            "replayed_bars": replayed,
            "missing_bars": len(missing),
        }
        if missing:
            payload["missing_first"] = missing[0].isoformat()
            payload["missing_last"] = missing[-1].isoformat()
        text = (
            "行情缺口处置 action=%s reason=%s 起=%s 止=%s 应有 %d 根 补派 %d 根 缺 %d 根"
            % (action, reason or "-",
               gap_start.strftime("%Y-%m-%d %H:%M:%S") if gap_start else "-",
               gap_end.strftime("%Y-%m-%d %H:%M:%S") if gap_end else "-",
               expected, replayed, len(missing))
        )
        payload["message"] = text
        if action == "replay":
            logger.info("[bar-gap] %s (session %s)", text, self.session_id)
        else:
            logger.warning("[bar-gap] %s (session %s)", text, self.session_id)
        self._emit("risk", payload)

    def _record_tick_profile(self, now: datetime, prof: Dict[str, int],
                             t_round: float) -> None:
        """记录本轮分段耗时（P2-8 观测）；轮次贴近/超过轮询间隔时告警 + 风险事件。

        - ``self._last_tick_profile`` 供测试/排查读取（最近一轮各段 ms）；
        - 阈值 = 轮询间隔的 85%（60s 间隔 → 51s），命中后每 5 分钟最多告警一次，
          避免每轮刷屏；``gate=loop_saturation`` 让工作台能看到「主循环饱和」。
        本方法只观测，不改变任何交易决策。
        """
        total = int((time.monotonic() - t_round) * 1000)
        prof["total"] = total
        self._last_tick_profile = prof
        self._tick_profile_total_ms = total
        interval = max(1.0, float(self._poll_interval or 0))
        if total < interval * 1000 * 0.85:
            return
        if (self._tick_saturation_warned_at is not None
                and (now - self._tick_saturation_warned_at).total_seconds() < 300):
            return
        self._tick_saturation_warned_at = now
        top = sorted(((k, v) for k, v in prof.items() if k != "total"),
                     key=lambda kv: kv[1], reverse=True)[:3]
        detail = ", ".join("%s=%dms" % (k, v) for k, v in top)
        logger.warning(
            "[tick-slow] round %dms ≥85%% of %ss interval (%s) (session %s)",
            total, self._poll_interval, detail or "-", self.session_id)
        self._emit("risk", {
            "session_id": self.session_id,
            "engine": self._engine_key,
            "gate": "loop_saturation",
            "action": "notice",
            "rule": "tick_too_slow",
            "round_ms": total,
            "interval_s": self._poll_interval,
            "top_segments": detail,
            "message": "主循环单轮耗时 %dms 已贴近轮询间隔 %ss（%s）"
                       % (total, self._poll_interval, detail or "-"),
        })

    def _tick_main(self) -> None:
        """_loop 单轮同步体（在 worker 线程执行）：心跳 → poll → 日终。

        异常在此捕获（非 CancelledError）——与原 _loop 行为一致：记日志不外抛，
        下一轮继续。BridgeUnavailableError → 置离线，本轮跳过。
        """
        now = now_shanghai()
        self._last_tick_at = now  # 存活心跳（进程内 watchdog 读）
        # P2-8：分段耗时画像（ms）。只观测不改执行顺序——风控/对账/清扫一律保留。
        t_round = time.monotonic()
        prof: Dict[str, int] = {}

        def _seg(name: str, fn):
            t0 = time.monotonic()
            try:
                return fn()
            finally:
                prof[name] = prof.get(name, 0) + int((time.monotonic() - t0) * 1000)

        try:
            if self.execution_mode == "alert":
                _seg("alert", lambda: self._tick_alert(now))
                return
            # E8：心跳前的在线状态，用于离线→在线转场时重建基线
            was_online = self._bridge_online
            if not _seg("heartbeat", self._dispatcher.heartbeat):
                self._mark_bridge_offline(now)
                logger.warning("bridge offline, pause trading (session %s)", self.session_id)
                # P1（2026-11-09）：桥离线 ≠ 当日收盘任务可跳过。日终估值与日线级别
                # 轮次只依赖本地快照/DB（tdx/ths/预警走 TQ 本地数据），照常执行，
                # 否则当天 1d/1w/1mon 信号与 daily_loss/熔断推进被静默吞掉一整天。
                self._run_daily_schedule()
                return
            self._bridge_online = True
            if not was_online:
                # E8：离线恢复。P2-6：行情源可回补历史 bar 时**不**重建基线，改为
                # 按缺口长度放大 count 幂等补派（见 _plan_gap_recovery）；快照合成源
                # 客观取不到断线期 bar，仍重建基线并记可审计缺口事件。
                self._recover_after_offline(now)
            # 次日盘前（集合竞价）执行日线信号单（默认关闭）：必须在行情 poll
            # 之前——09:15 一开窗就把昨日 deferred 日线单报进集合竞价。关闭时
            # 方法首行 return，队列/DB/价格/事件一律不动（旧行为逐字不变）。
            self._maybe_pre_open_auction(now)
            # poll() 内部对每根完成的 bar 触发 self._on_bar 回调
            # 行情轮询窗口（收盘后停拉行情）：窗口外（交易日 15:05 后 / 次日 09:15 前 /
            # 非交易日）整轮不拉行情、不注入外部信号——这是收盘后对掘金桥 /quote
            # 与 TQ 压力最大的来源。暂停期间以下链路照常：心跳、账户快照采集与
            # 对账导入（_tick_deals）、超时收割/日终/收盘清扫/收盘对账复查。
            if self._quote_poll_active(now):
                if self._quote_poll_paused:
                    self._quote_poll_paused = False
                    self._leave_quote_poll_pause(now)
                # 掘金通道：桥 1m「今日已完成 bar」探测 → 立即放行 deferred，
                # 不等快照合成的首根完成 bar（其它通道该方法直接 return）
                self._maybe_bridge_deferred_release(now)
                if self._pending_backfill_count:
                    # P2-6：断线恢复首轮，用放大后的 count 一次取回缺口 bar，按
                    # per-code 水位补派；补派同样走 _on_bar 全套闸门（新鲜度/时段/
                    # bar 级幂等键），不绕过任何检查。
                    count = self._pending_backfill_count
                    self._pending_backfill_count = 0
                    try:
                        triggered = _seg(
                            "poll_backfill",
                            lambda: self._bar_poller.poll(backfill_count=count))
                    except BridgeUnavailableError:
                        # 桥又断了：交给外层 _mark_bridge_offline（行为不变）
                        raise
                    except Exception:  # noqa: BLE001
                        # P1-3（2026-11-12）：旧实现先清零再 poll，非 Bridge* 异常
                        # 抛出后计数已丢 = 本次补派永久放弃、静默丢缺口。现在保留
                        # 计数（下一轮重试），并按缺口计划只落一条 retry 事件。
                        self._pending_backfill_count = count
                        if not self._backfill_retry_reported:
                            self._backfill_retry_reported = True
                            plan = self._gap_recovery_plan or {}
                            self._emit("risk", {
                                "session_id": self.session_id,
                                "engine": self._engine_key,
                                "gate": "bar_gap",
                                "action": "retry",
                                "rule": "backfill_failed",
                                "gap_start": (
                                    plan["gap_start"].isoformat()
                                    if plan.get("gap_start") else None),
                                "gap_end": (
                                    plan["gap_end"].isoformat()
                                    if plan.get("gap_end") else None),
                                "count": count,
                                "message": ("缺口补派失败（非桥离线异常），保留 %d 根"
                                            "计数下一轮重试；缺口审计将延后出具"
                                            % count),
                            })
                        logger.exception(
                            "gap backfill failed, will retry (session %s)",
                            self.session_id)
                        raise
                    self._audit_gap_recovery(now, triggered)
                else:
                    _seg("poll", self._bar_poller.poll)
                # 0011 外部信号注入：poll 之后增量读取通达信条件预警文件，公式名匹配
                # 策略的信号逐条注入（同样走 _handle_bar 全套，与 bar 驱动同闸门链）。
                _seg("external_signals", self._tick_external_signals)
            elif not self._quote_poll_paused:
                self._quote_poll_paused = True
                self._enter_quote_poll_pause(now)
            # P2：超时检查移主循环——deals 循环被 60s 主循环饿死（单 worker 串行，
            # 主循环拉 ~73 只行情耗 50-57s/轮），180s 超时实际 440s 才跑到。移到主循环
            # 60s 节拍，最坏 60s 延迟。#1 修复后受理即丢弃已即时 rejected，此超时退为
            # 兜底（桥未丢但 order_ref 迟迟匹配不上的边缘场景）。同 _persist_breaker_count
            # 的 db session 模式；_expire_stale_orders 自身不 commit，由本块负责。
            db_tick = self._db_session_factory()
            try:
                _seg("sweep_timeout", lambda: self._sweep_timeout_orders(db_tick))
            except Exception:  # noqa: BLE001
                db_tick.rollback()
                logger.exception("expire stale orders error (session %s)", self.session_id)
            finally:
                db_tick.close()
            # E5/E6 日终估值 + 15:05 日线级别轮次 + B 14:52 提前收盘价轮次，
            # 合并为心跳门控外的调度段（见 _run_daily_schedule：单段失败不影响其余段）。
            _seg("daily_schedule", self._run_daily_schedule)
            # 15:05 收盘清扫：交易日收盘后对仍未完结的单做确定性兜底（成交回填 +
            # 终态同步 + 剩余按 A 股收盘未成交=撤单标 canceled）。实时轮询的权威收口。
            _seg("close_sweep", self._maybe_close_sweep)
            # I-7（audit）：收盘流程完成后每交易日一次对账复查（虚拟 vs 桥实际）——
            # 此时在途单已终态收敛、成交已回填，差异最可信。
            _seg("reconcile_after_close", self._maybe_reconcile_after_close)
        except BridgeUnavailableError as e:
            self._mark_bridge_offline(now)
            logger.warning("bridge unavailable: %s, skip this round", e)
        except Exception:  # noqa: BLE001
            logger.exception("live loop unexpected error")
        finally:
            # P2-8：记录本轮耗时画像/超时告警（观测，不改变本轮已执行内容）
            self._record_tick_profile(now, prof, t_round)

    def _maybe_pre_open_auction(self, now: datetime) -> None:
        """次日盘前（集合竞价）执行「当日 due」的 deferred 日线信号单（默认关闭）。

        与 09:31 首根 bar 路径的关系（本需求最容易出错的地方）：
        - 开关关闭 → 本方法首行 return；deferred 队列、DB 行、价格、事件全不动，
          完全走既有「次一交易日首根完成 1m bar 放行」逻辑，行为逐字不变。
        - 开关开启 → 仅在交易日 09:15 ≤ now < end_time 的窗口内，把 due=今日 的
          deferred **日线**单从内存队列摘出（等价 bar 路径的放行动作），按
          「信号价 ± bias% 并夹进当日涨跌停」定价后，走**同一条**
          `_submit_one_order` 闸门链（资金/整手/在途预留/审批/T+1/熔断/风控
          优先级全部复用，不另起一套）。不依赖任何 1 分钟 bar，不需快照合成。
        - 幂等/at-most-once：①每单每日只尝试一次（`_pre_open_attempted` 用逻辑
          键，与 DB 行 id 解耦——失败后重落 deferred 行换了 id 也不会二次尝试）；
          ②真正防重复建仓靠提交链自身「先删 deferred 行 + 落 submitted 行，再发
          桥」的次序：已报出（含受理未确认，行 status=submitted/partial）→ 不回
          队，09:31 不会再报；闸门拦截/异常回滚/桥明确拒单 → 行仍 deferred（或按
          原 signal bar_time 重落 deferred 行）并放回内存队列，由 09:31 常规路径
          重试。最坏结果是「盘前没报成、09:31 报一次」，不存在两次都报。
        """
        cfg = self._pre_open
        if not cfg.enabled or self.execution_mode != "auto":
            return
        if not cfg.is_open(now):
            return
        today = now.date()
        try:
            if not self._calendar.is_trading_day(today):
                return
        except Exception:  # noqa: BLE001
            logger.exception(
                "pre-open calendar check failed (session %s)", self.session_id)
            return
        # 通道能力闸（QMT 特有问题）：盘前委托价由平台算出，只有「平台确实掌握
        # 委托价」的通道才能报。判不出 / 桥会把 limit 回退成对手价 → fail-closed
        # 拦截，绝不静默按对手价报集合竞价；deferred 单顺延到 09:31 首根 bar。
        block_reason = self._pre_open_price_block_reason()
        if block_reason is not None:
            if self._pre_open_blocked_day != today:
                self._pre_open_blocked_day = today
                msg = "盘前集合竞价不放行：%s；deferred 单顺延至 09:31" % block_reason
                logger.warning(
                    "[pre-open] blocked: %s (session %s)", msg, self.session_id)
                self._emit("risk", {
                    "session_id": self.session_id,
                    "engine": self._engine_key,
                    "gate": "pre_open_auction",
                    "action": "block",
                    "rule": "pre_open_price_capability",
                    "channel": getattr(self._dispatcher, "channel", ""),
                    "message": msg,
                })
            return
        if self._pre_open_day != today:
            self._pre_open_day = today
            self._pre_open_attempted.clear()
            # 回显异常按日重新取证（桥当天修复/重启后次日恢复，不跨日残留）
            self._pre_open_echo_bad = None
        # 放行截止（2026-11-11）：截止 = 窗口结束 - 通道余量。过点不再放新单，未放行
        # 的保持 deferred，由 09:31 首根 1m bar 兜底——避免「窗口后半段才报出去、
        # 桥/UI 还没受理就到了 09:20/09:25 不可撤也不可申报」的死角。
        margin = self._pre_open_release_margin(len(self._signal_bar_deferred))
        cutoff = datetime(
            today.year, today.month, today.day, cfg.end_time[0], cfg.end_time[1]
        ) - timedelta(seconds=margin)
        if now >= cutoff:
            if self._pre_open_deadline_day != today:
                self._pre_open_deadline_day = today
                msg = (
                    "盘前集合竞价放行已过截止时刻 %s（窗口结束 %02d:%02d − 通道余量 "
                    "%ds），剩余 deferred 单顺延至 09:31"
                    % (cutoff.strftime("%H:%M:%S"), cfg.end_time[0],
                       cfg.end_time[1], margin)
                )
                logger.warning(
                    "[pre-open] release window closed: %s (session %s)",
                    msg, self.session_id)
                self._emit("risk", {
                    "session_id": self.session_id,
                    "engine": self._engine_key,
                    "gate": "pre_open_auction",
                    "action": "block",
                    "rule": "release_deadline",
                    "channel": getattr(self._dispatcher, "channel", ""),
                    "margin_seconds": margin,
                    "cutoff": cutoff.strftime("%H:%M:%S"),
                    "message": msg,
                })
            return
        self._pre_open_release_cutoff = cutoff
        for portfolio in list(self.portfolios):
            try:
                self._pre_open_release(portfolio, now)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "pre-open release failed (session %s, portfolio %s)",
                    self.session_id, portfolio.portfolio_id)

    def _pre_open_release_margin(self, batch_size: int) -> int:
        """放行截止余量（秒）：按通道类型，不写死引擎名。

        UI 自动化通道（ThsUiDispatcher 有 execute_mode）每单要定位字段 + 键入价格 +
        等 4s 防现价覆盖 + 处置确认弹窗，按 25s/单估算；HTTP 桥（QMT/掘金）一次往返
        即受理，30s 足够。
        """
        if hasattr(self._dispatcher, "execute_mode"):
            return min(
                _PRE_OPEN_UI_MARGIN_MAX,
                max(_PRE_OPEN_UI_MARGIN_MIN,
                    _PRE_OPEN_UI_PER_ORDER * max(1, int(batch_size))),
            )
        return _PRE_OPEN_BRIDGE_MARGIN

    def _pre_open_price_type(self) -> Optional[str]:
        """盘前单按单指定的委托价类型：通道自述 supports_price_type("limit") 才传。

        判据只看 dispatcher 能力（QMT/掘金同构 HTTP 桥都实现 supports_price_type），
        不写死引擎名；通达信/同花顺天然掌握委托价，返回 None（不传该参数，原行为）。
        """
        supports = getattr(self._dispatcher, "supports_price_type", None)
        if not callable(supports):
            return None
        try:
            return "limit" if supports("limit") is True else None
        except Exception:  # noqa: BLE001
            return None

    def _pre_open_price_block_reason(self) -> Optional[str]:
        """盘前竞价通道能力闸：None = 允许进入盘前路径；字符串 = 拦截原因。

        盘前单的委托价是平台算出来的（信号价 ± bias% 再夹涨跌停），只有「平台确实
        掌握委托价」的通道才能报：通达信 PRICE_MY、同花顺盘前强制 approval、掘金桥
        OrderType_Limit 天然满足（既有行为不变）。

        QMT 桥（2026-11-10 起）：盘前单**按单**显式带 ``price_type=limit``
        （prType=11，审批价原样下发），盘中单仍用配置默认（opponent），因此不再要求
        会话级 ``price_type: limit``——那个代价是盘中单也会被切成限价、可能挂一天
        不成交。放行只判两条：
        ①桥进程自述支持指定价（``supports_price_type("limit") is True``：/ping 声明
          或含 /price_limit 端点；未升级的桥会忽略该字段、静默按 prType=14 报出）；
        ②本会话的**盘前单**未被观测到回显非 limit（``_pre_open_echo_bad``，只采盘前
          单的 /order 应答，不会因盘中单的 opponent 回显误拦）。
        任一不满足 → 拦截（绝不静默降级成对手价），deferred 单顺延 09:31 首根 bar。
        """
        supports = getattr(self._dispatcher, "supports_price_type", None)
        if not callable(supports):
            # 非桥通道（通达信 PRICE_MY / 同花顺 approval 键入）各自掌握委托价：
            # 不做指定价能力校验（原行为）。桥通道（QMT iquant / 掘金 goldminer
            # 同构 HttpBridgeDispatcher）才受此闸约束。
            return None
        capable = None
        try:
            capable = supports("limit")
        except Exception:  # noqa: BLE001
            logger.warning(
                "[pre-open] probe bridge price_type failed (session %s)",
                self.session_id, exc_info=True)
            capable = None
        if capable is not True:
            return ("桥不支持指定价（/ping 未确认 price_type=limit 能力——"
                    "老桥 prType 固定 14、price 被丢弃）；盘前限价不可控")
        if self._pre_open_echo_bad is not None:
            return ("盘前单 /order 回显委托价类型为 %r（非 limit，桥未采纳指定价），"
                    "盘前限价不可控" % self._pre_open_echo_bad)
        return None

    def _on_claim_anomaly(self, payload: dict) -> None:
        """通道认领异常（TDX PRICE_MY 价差不符）→ risk 事件，工作台可见。"""
        if not isinstance(payload, dict):
            return
        body = dict(payload)
        body.setdefault("session_id", self.session_id)
        body.setdefault("engine", self._engine_key)
        self._emit("risk", body)

    def _announce_pre_open_price_mode(self) -> None:
        """启动声明：盘前竞价窗口内同花顺被强制 approval（价格由平台键入）。"""
        if not getattr(self._dispatcher, "price_mode_forced_pre_open", False):
            return
        msg = ("盘前竞价窗口（09:15-09:20）：同花顺强制 approval 价格模式，"
               "价格由平台键入（无视 ths_ui.price_mode）")
        logger.warning("%s (session %s)", msg, self.session_id)
        self._emit("decision", {
            "session_id": self.session_id,
            "engine": self._engine_key,
            "gate": "pre_open_price_mode",
            "action": "forced",
            "channel": getattr(self._dispatcher, "channel", ""),
            "price_mode": getattr(self._dispatcher, "ths_price_mode", "approval"),
            "message": msg,
        })

    def _pre_open_release(self, portfolio: Portfolio, now: datetime) -> None:
        """单组合的盘前放行：摘单 → 定价 → 批量资金分配 → 逐单提交 → 未报出回队。"""
        today = now.date()
        candidates: List[OrderEvent] = []
        with self._book_lock:
            for order in list(self._signal_bar_deferred):
                if order.portfolio_id != portfolio.portfolio_id:
                    continue
                if order.bar_time is None or order.signal_bar_date is None:
                    continue
                # 已顺延一次的单（due 已后移）不参与本日盘前
                if getattr(order, "_deferred_extended", False):
                    continue
                # 只处理「当日 due」：signal_bar_date 的下一交易日 == 今天
                if self._next_trading_day(order.signal_bar_date) != today:
                    continue
                # 日线级（1d/1w/1mon）全部覆盖：1w/1mon 的 due 判定同样是
                # signal_bar_date 的下一交易日，与日线同口径参与集合竞价。
                # P1-3/P1-4：分钟级重排单（signal_bar_date 由重排写入）不进本窗口，
                # 交由次一交易日的首根 1m bar 路径（分钟单不受集合竞价时段闸支持，
                # 也不该占一次盘前尝试）。
                op = self._order_period(portfolio, order)
                if op and op.lower() not in _DAILY_PERIODS:
                    continue
                key = self._pre_open_key(portfolio, order)
                if key in self._pre_open_attempted:
                    continue
                self._pre_open_attempted.add(key)
                candidates.append(order)
            if candidates:
                # 按身份摘除（OrderEvent 是 dataclass，remove 会按字段相等误删同值单）
                self._signal_bar_deferred = [
                    o for o in self._signal_bar_deferred
                    if not any(o is c for c in candidates)
                ]
        if not candidates:
            return
        db = self._db_session_factory()
        requeue: List[OrderEvent] = []
        try:
            # SELL 先于 BUY（与 _handle_bar 同口径）：卖出回款先于买单审批
            orders = sorted(
                candidates,
                key=lambda o: 0 if o.trade_type == TradeType.SELL else 1)
            for order in orders:
                order._pre_open_bar_time = order.bar_time  # 原信号 bar 时间（回队恢复）
                order.bar_time = now  # 决策时间戳语义：本次盘前报单时点
                price = self._pre_open_price(order)
                if price is not None:
                    order.price = price
                self._rec_live(
                    "pre_open_auction", "release", portfolio, order,
                    message="盘前集合竞价放行：signal_bar_date=%s，委托价 %s"
                            "（信号价让价 %s%%，涨跌停内收 1 跳）"
                            % (order.signal_bar_date,
                               fmt_price(order.price),
                               self._pre_open.price_bias_pct),
                )
                logger.info(
                    "[pre-open] release %s %s qty=%s price=%s signal_bar_time=%s "
                    "cutoff=%s pre_open=True (session %s)",
                    order.trade_type.value, order.stock_code, order.quantity,
                    fmt_price(order.price), order._pre_open_bar_time,
                    (self._pre_open_release_cutoff.strftime("%H:%M:%S")
                     if self._pre_open_release_cutoff else "-"),
                    self.session_id,
                )
            orders = self._allocate_batch_buys(portfolio, orders, post_close=False)
            allocated_ids = {id(o) for o in orders}
            for order in candidates:
                if id(order) not in allocated_ids:
                    # 同批资金分配剔除（分配后不足 1 手）：保持 deferred 行，
                    # 回队等 09:31 首根 bar 路径按当轮资金重新评估
                    order.bar_time = order._pre_open_bar_time
                    requeue.append(order)
                    continue
                if self._pre_open_echo_bad is not None:
                    # 本轮已有盘前单被桥回显非 limit（未采纳指定价）：同批后续单
                    # 立即回队，不再报出（下一轮 60s 由能力闸统一拦截）
                    order.bar_time = order._pre_open_bar_time
                    self._requeue_pre_open(db, order)
                    requeue.append(order)
                    continue
                try:
                    # period：按订单实际周期（1d/1w/1mon 都在 _DAILY_PERIODS 内，
                    # 跳过分钟墙钟闸/收盘守卫）；emit_signal=False——信号事件昨日
                    # 已由收盘轮次推过，盘前只补「放行/报单」审计。
                    # price_type：QMT 桥**按单**显式指定 limit（prType=11，审批价
                    # 原样下发），不影响盘中单（仍走配置默认 opponent）；其它通道
                    # 天然掌握委托价，不传该参数（保持原行为）。
                    pre_open_price_type = self._pre_open_price_type()
                    if pre_open_price_type is not None:
                        # 清掉上一笔（可能是盘中单）的回显，只采本次盘前单的应答
                        try:
                            self._dispatcher.last_order_price_type = None
                        except Exception:  # noqa: BLE001
                            pass
                    try:
                        self._submit_one_order(
                            db, portfolio, order,
                            self._order_period(portfolio, order) or "1d",
                            emit_signal=False,
                            price_type=pre_open_price_type)
                    finally:
                        if pre_open_price_type is not None:
                            echo = getattr(
                                self._dispatcher, "last_order_price_type", None)
                            if echo is not None and str(echo).strip().lower() != "limit":
                                # 兜底校验（只针对盘前单）：桥未采纳 limit → 后续
                                # 盘前单整日拦截，绝不静默按对手价报出
                                self._pre_open_echo_bad = str(echo)
                                logger.error(
                                    "[pre-open] bridge echoed price_type=%r (expected "
                                    "limit) — pre-open blocked for the rest of the day "
                                    "(session %s)", echo, self.session_id)
                except Exception:  # noqa: BLE001
                    db.rollback()
                    logger.exception(
                        "[pre-open] submit failed, keep deferred for 09:31 path: "
                        "%s %s qty=%s (session %s)",
                        order.trade_type.value, order.stock_code,
                        order.quantity, self.session_id)
                    self._rec_live(
                        "pre_open_auction", "block", portfolio, order,
                        message="盘前提交异常，保持 deferred 交由首根 1m bar 路径重试",
                    )
                if self._pre_open_placed(db, order):
                    # 已真正报出（或已交给拆单队列承接）：不回队，防 09:31 重复
                    if getattr(order, "_deferred_row_id", None) is not None:
                        self._drop_pre_open_deferred_row(db, order)
                    # 登记「开盘后越价校验」（P5）：限价取本次实际委托价，
                    # 开盘后首根 1m bar 上若开盘价越过该限价 → 主动撤单。
                    self._pre_open_open_checks.append({
                        "day": today,
                        "code": order.stock_code,
                        "side": order.trade_type,
                        "limit": Decimal(str(order.price)),
                        "strategy_id": order.strategy_id,
                        # P1-4：撤单后可重排——保留订单对象，撤单确认后回 deferred，
                        # 交同一交易日 09:31 首根 1m 路径按真实开盘价重定限价重试。
                        "order": order,
                    })
                    continue
                # 未报出：恢复原 signal bar_time 并放回队列（09:31 常规路径重试）
                order.bar_time = order._pre_open_bar_time
                self._requeue_pre_open(db, order)
                requeue.append(order)
            self._drain_decisions(db, portfolio)
        finally:
            db.close()
        if requeue:
            with self._book_lock:
                existing = {id(o) for o in self._signal_bar_deferred}
                for order in requeue:
                    if id(order) not in existing:
                        self._signal_bar_deferred.append(order)
            logger.info(
                "[pre-open] requeued %d order(s) for 09:31 first-bar path "
                "(session %s)", len(requeue), self.session_id)

    @staticmethod
    def _pre_open_key(portfolio: Portfolio, order: OrderEvent) -> tuple:
        """盘前幂等键（逻辑单身份，不含 DB 行 id / 数量——重落行、缩量后仍同一单）。"""
        return (
            portfolio.portfolio_id, order.strategy_id, order.stock_code,
            order.trade_type.value, order.signal_bar_date,
        )

    def _check_pre_open_orders_at_open(self, bar: BarEvent) -> None:
        """盘前已报出单的开盘越价校验（P5，2026-11-11）。

        集合竞价按统一开盘价撮合：买单限价 < 开盘价 / 卖单限价 > 开盘价时，该单
        在开盘价上必然不成交，只能以昨收信号推出来的限价挂在连续竞价簿上——等于
        全天被动挂单，因此开盘后主动撤出。

        **2026-11-11（P1-4）取舍反转**：旧版「撤单且不自动重报」，等于把一笔已决策
        交易直接作废；而回测按次日开盘价**无条件成交**，两端口径不一致（实盘系统性
        少做）。现改为：撤单**确认**后回 deferred，由同一交易日 09:31 首根 1m bar
        路径按真实开盘价重定限价重试，受 `_DEFERRED_RETRY_MAX` 次数上限约束；
        撤单未确认（accepted/pending/failed）时绝不重排（防双份成交）。

        时点：开盘后首根**完成** 1m bar（≥09:30）。09:15–09:20 可撤、**09:20–09:25
        是交易所集合竞价的不可撤单窗口**、09:25–09:30 无成交也无开盘价，因此本
        校验绝不在 09:25 前执行。

        P2-4（2026-11-11）：旧实现「先写每日完成标记，再比对」，首根 1m bar 迟到、
        或某只票在该 bar 里缺 open（停牌/停牌复牌首日/合成器丢采样）时，标记已被
        写死 → 当天永久漏撤。现在改为**逐单解决、完成才置位**：
        - 缺开盘价 → 保留登记，下一根 1m bar 重试；
        - 撤单未收敛（pending/failed）→ 保留登记重试；
        - 已比对（未越价 / 撤单确认 / 无在途可撤）→ 该单解决；
        - 全部解决才写完成标记；否则继续重试到 10:00（`_PRE_OPEN_OPEN_CHECK_DEADLINE`），
          到点仍未解决的按「无价可判」跳过并记 risk 事件（action=skip），不再无限重试。
        """
        if str(getattr(bar, "period", "")) != "1m":
            return
        bt = getattr(bar, "bar_time", None)
        if bt is None:
            return
        today = bt.date()
        if self._pre_open_open_check_day == today:
            return
        # P1-1（2026-11-12）：本方法由 `_handle_bar` 按组合调用，同一根 1m bar 会
        # 被调 P 次（P=组合数）。未收敛单若每份 bar 都撤一次，会重复发撤单请求、
        # 重复落 action=cancel 事件。同一根 bar 只跑一次（引擎级，跨组合共享）。
        if self._pre_open_open_check_bar == bt:
            return
        self._pre_open_open_check_bar = bt
        entries = [e for e in self._pre_open_open_checks
                   if e.get("day") == today]
        if not entries:
            return
        # 重试截止：10:00 之后不再校验（开盘价对撤单决策已无意义）
        deadline = (bt.hour, bt.minute) > _PRE_OPEN_OPEN_CHECK_DEADLINE
        stocks = getattr(bar, "stocks", None) or {}
        remaining: List[dict] = []
        unresolved_codes: List[str] = []
        for entry in entries:
            stock = stocks.get(entry["code"])
            open_px = getattr(stock, "open", None)
            if open_px is None and isinstance(stock, dict):
                open_px = stock.get("open")
            try:
                open_px = Decimal(str(open_px))
            except Exception:  # noqa: BLE001 缺价/停牌
                open_px = None
            if open_px is None or open_px <= 0:
                # 该 bar 里没有这只票的可信开盘价：留到下一根 1m bar 重试
                if not deadline:
                    remaining.append(entry)
                else:
                    unresolved_codes.append(entry["code"])
                continue
            limit = entry["limit"]
            crossed = (open_px > limit if entry["side"] == TradeType.BUY
                       else open_px < limit)
            if not crossed:
                continue  # 未越价：已解决
            if self._cancel_pre_open_order(entry, open_px):
                continue  # 撤单已收敛（含无在途可撤）：已解决
            # 撤单未收敛（pending/failed/异常）：保留登记，下一根 bar 再试
            if not deadline:
                remaining.append(entry)
            else:
                unresolved_codes.append(entry["code"])
        # 写回今日剩余登记（其它日期不动）：全部解决才置完成标记，否则下根 bar 重试
        self._pre_open_open_checks = [
            e for e in self._pre_open_open_checks if e.get("day") != today
        ] + remaining
        if remaining:
            return
        self._pre_open_open_check_day = today
        if unresolved_codes:
            msg = ("开盘越价校验到 %02d:%02d 截止仍未完成（缺可信开盘价或撤单未收敛）：%s"
                   % (_PRE_OPEN_OPEN_CHECK_DEADLINE[0],
                      _PRE_OPEN_OPEN_CHECK_DEADLINE[1],
                      "、".join(sorted(set(unresolved_codes)))))
            logger.warning("[pre-open] %s (session %s)", msg, self.session_id)
            self._emit("risk", {
                "session_id": self.session_id,
                "engine": self._engine_key,
                "gate": "pre_open_auction",
                "action": "skip",
                "rule": "open_check_timeout",
                "codes": sorted(set(unresolved_codes)),
                "message": msg,
            })

    def _cancel_pre_open_order(self, entry: dict, open_price: Decimal) -> bool:
        """撤掉「开盘即越过限价」的盘前单。

        返回 **该登记单是否已解决**（P2-4，供调用方决定「当天校验是否完成」）：
        - True：已比对完成——通道不支持撤单 / 无在途行 / 撤单确认 canceled
          （含撤单后成功回 deferred）；
        - False：撤单未收敛（accepted/pending/failed/异常），旧单可能仍在柜台撮合，
          需保留登记由下一根 1m bar 重试；绝不在此状态重排（防双份委托）。
        """
        cancel = getattr(self._dispatcher, "cancel_order", None)
        if not callable(cancel):
            return True
        # 老桥（无 /cancel 端点）404 后 dispatcher 会置 supports_cancel=False：
        # 之后逐单再发只会刷失败日志，直接按「通道不支持撤单」跳过。
        if getattr(self._dispatcher, "supports_cancel", True) is False:
            return True
        db = self._db_session_factory()
        try:
            row = (
                db.query(LiveOrder)
                .filter(
                    LiveOrder.live_session_id == self.session_id,
                    LiveOrder.strategy_id == entry["strategy_id"],
                    LiveOrder.stock_code == entry["code"],
                    # DB 落库为小写（order_machine: trade_type.value.lower()）；
                    # TradeType.SELL.value 是大写 "SELL"，直接比会永不命中 → 不撤单。
                    LiveOrder.trade_type == str(
                        getattr(entry["side"], "value", entry["side"])).lower(),
                    LiveOrder.status.in_(("submitted", "partial")),
                )
                .order_by(LiveOrder.id.desc())
                .first()
            )
            if row is None or not getattr(row, "order_ref", None):
                # 已无在途可撤（已成交/已撤/已终态）：该登记单无需再处理
                return True
            # 撤单结果判定（P1-4 安全阀）：只有**确认撤成**才允许重排。
            # accepted（桥受理但未观测到终态）/pending/failed 一律不重排——旧单可能
            # 仍在柜台撮合，重排会变成双份委托（真金白银）。last_cancel_result 先清空，
            # 避免读到上一笔撤单的残留值。
            try:
                self._dispatcher.last_cancel_result = None
            except Exception:  # noqa: BLE001
                pass
            try:
                cancel_ok = bool(cancel(row.order_ref, row.stock_code))
            except Exception:  # noqa: BLE001
                logger.exception(
                    "[pre-open] cancel crossed order failed: ref=%s %s "
                    "(session %s)", row.order_ref, row.stock_code, self.session_id)
                # 撤单异常：柜台状态未知 → 保留登记重试（绝不在此状态重排）
                return False
            outcome = getattr(self._dispatcher, "last_cancel_result", None)
            if getattr(self._dispatcher, "supports_cancel", True) is False:
                # 本次尝试才发现通道不支持撤单（老桥 404）：再试只会刷失败日志，
                # 登记直接解决，退回 180s 超时 expire 兜底（与旧行为一致）
                return True
            confirmed = cancel_ok and outcome in (None, "canceled")
            logger.warning(
                "[pre-open] 开盘价 %s 越过限价 %s，撤单 %s %s ref=%s "
                "confirmed=%s (session %s)",
                fmt_price(open_price), fmt_price(entry["limit"]),
                entry["side"].value, entry["code"], row.order_ref,
                confirmed, self.session_id)
            requeued = False
            if confirmed:
                requeued = self._requeue_open_crossed_order(
                    db, entry, row, open_price)
            if requeued:
                return True
            # P1-5（2026-11-12）：桥返回 accepted = 撤单已受理但终态未观测到，
            # **不是已解决**。旧实现因 dispatcher 对 accepted 返回 True 而被
            # `return bool(cancel_ok)` 当成已解决 → 登记单被摘除、再无撤单机会。
            # 现在保留登记交给下一根 1m bar 再撤，并留痕（每单只落一条）。
            if outcome == "accepted":
                if not entry.get("cancel_accepted_reported"):
                    entry["cancel_accepted_reported"] = True
                    self._emit("risk", {
                        "session_id": self.session_id,
                        "engine": self._engine_key,
                        "gate": "pre_open_auction",
                        "action": "cancel",
                        "rule": "open_crossed_limit",
                        "status": "accepted",
                        "code": entry["code"],
                        "side": entry["side"].value,
                        "limit_price": float(entry["limit"]),
                        "open_price": float(open_price),
                        "order_ref": row.order_ref,
                        "message": ("开盘价已越过盘前限价，撤单已被受理但未观测到"
                                    "终态 → 保留登记，下一根 1m bar 再撤"),
                    })
                return False
            # 同一登记单的撤单结论事件只落一次（未收敛时会跨 bar 重试，避免刷屏）
            if not entry.get("cancel_reported"):
                entry["cancel_reported"] = True
                self._emit("risk", {
                    "session_id": self.session_id,
                    "engine": self._engine_key,
                    "gate": "pre_open_auction",
                    "action": "cancel",
                    "rule": "open_crossed_limit",
                    "code": entry["code"],
                    "side": entry["side"].value,
                    "limit_price": float(entry["limit"]),
                    "open_price": float(open_price),
                    "order_ref": row.order_ref,
                    "message": ("开盘价已越过盘前限价，判定必然未成交，主动撤单；"
                                "撤单未确认或重排次数用尽 → 不重报"),
                })
            # 撤单已确认但未重排（次数用尽/无订单对象）→ 旧单已撤，登记解决；
            # 撤单未确认（pending/failed/传输失败）→ 保留登记下一根 bar 再试。
            # not_found/rejected 视为已解决：柜台已无此单或明确拒撤，再试也无意义。
            return bool(cancel_ok) or outcome in ("not_found", "rejected")
        finally:
            db.close()

    def _requeue_open_crossed_order(self, db: Session, entry: dict,
                                    row: LiveOrder, open_price: Decimal) -> bool:
        """P1-4：盘前越价撤单确认后把单放回 deferred（受重排次数上限约束）。

        与旧取舍的差异（2026-11-11 反转）：此前「开盘越价 → 撤单且不自动重报」，
        理由是「越价说明这笔价不想要了」。但回测按次日开盘价**无条件成交**，只撤
        不重报 = 系统性少做一笔已决策交易。改为：撤单后回 deferred，由**同一交易日
        09:31 首根 1m bar** 路径按真实开盘价重定限价再报（`_refresh_release_price`
        取当日首根 open，夹涨跌停）——既不是盲目挂昨收信号价，也不追价；次数受
        `_DEFERRED_RETRY_MAX` 约束，且撤单未确认时绝不重排（防双份成交）。
        """
        order = entry.get("order")
        if order is None:
            return False
        count = int(getattr(order, "_deferred_retry_count", 0) or 0)
        if count >= _DEFERRED_RETRY_MAX:
            return False
        count += 1
        order._deferred_retry_count = count
        prev_bar_time = getattr(order, "_pre_open_bar_time", None)
        if prev_bar_time is not None:
            # 恢复原 signal bar_time：早于今日 → 同一根 _on_bar 内即被放行
            order.bar_time = prev_bar_time
        if order.signal_bar_date is None and order.bar_time is not None:
            order.signal_bar_date = order.bar_time.date()
        # 限价由放行路径重定（首根 open）；清掉盘前 limit 覆盖与盘前标记
        order._price_type_override = None
        order._pre_open_bar_time = None
        order._deferred_row_id = row.id
        order._requeued_for_retry = True
        row.status = "deferred"
        row.error_message = (
            "%s%d; open_crossed_limit: 开盘价 %s 越过盘前限价 %s"
            % (_RETRY_MARKER, count, fmt_price(open_price),
               fmt_price(entry["limit"])))[:500]
        with self._book_lock:
            self._pending_orders.pop(row.id, None)
            if all(id(o) != id(order) for o in self._signal_bar_deferred):
                self._signal_bar_deferred.append(order)
            db.commit()
        logger.warning(
            "[pre-open] 越价撤单已确认 → 回 deferred，同一交易日 09:31 首根 1m 路径"
            "按真实开盘价重定限价重试（第 %d/%d 次尝试）%s ref=%s (session %s)",
            count + 1, _DEFERRED_RETRY_MAX + 1, entry["code"],
            row.order_ref, self.session_id)
        msg = (
            "开盘价已越过盘前限价，撤单后**回 deferred**：本次改为在同一交易日 "
            "09:31 首根 1m bar 上按真实开盘价重定限价重试，受 %d 次重排上限约束"
            "（不是终态）。动机：回测按次日开盘价无条件成交，只撤不重报会系统性"
            "少做一笔已决策交易；重定限价后不追价、不挂昨收信号价。"
            % _DEFERRED_RETRY_MAX)
        portfolio = next(
            (p for p in self.portfolios
             if p.find_strategy(entry.get("strategy_id")) is not None), None)
        if portfolio is not None:
            self._rec_live("pre_open_auction", "cancel_requeue", portfolio, order,
                           message=msg)
        self._emit("risk", {
            "session_id": self.session_id,
            "engine": self._engine_key,
            "gate": "pre_open_auction",
            "action": "cancel_requeue",
            "rule": "open_crossed_limit",
            "code": entry["code"],
            "side": entry["side"].value,
            "limit_price": float(entry["limit"]),
            "open_price": float(open_price),
            "order_ref": row.order_ref,
            "attempt": count + 1,
            "max_attempts": _DEFERRED_RETRY_MAX + 1,
            "message": msg,
        })
        return True

    @staticmethod
    def _order_period(portfolio: Portfolio, order: OrderEvent) -> Optional[str]:
        """订单所属周期：优先 OrderEvent.period，其次策略上下文 period。"""
        period = getattr(order, "period", None)
        if period:
            return period
        ctx = portfolio.find_strategy(order.strategy_id)
        return getattr(ctx, "period", None) if ctx is not None else None

    def _pre_open_placed(self, db: Session, order: OrderEvent) -> bool:
        """盘前委托是否已真正落到柜台（submitted/partial，含「受理未确认」）。

        查询失败按「已报出」保守处理——宁可漏一次 09:31 重试，绝不重复建仓。
        大额拆单命中时父单本身无行（已转 4 笔子单队列），同样视为已报出。
        """
        key = (order.strategy_id, order.stock_code)
        entry = self._split_children.get(key)
        if entry is not None:
            inflight = entry.get("inflight")
            if (inflight is not None
                    and getattr(inflight, "trade_type", None) == order.trade_type
                    and inflight.bar_time is not None
                    and order.bar_time is not None
                    and (inflight.bar_time.year, inflight.bar_time.month,
                         inflight.bar_time.day, inflight.bar_time.hour,
                         inflight.bar_time.minute)
                    == (order.bar_time.year, order.bar_time.month,
                        order.bar_time.day, order.bar_time.hour,
                        order.bar_time.minute)):
                return True
        if getattr(order, "_deferred_row_id", None) is not None:
            # 提交链未删除 deferred 行 = 未走到落 submitted 行（闸门拦截/回滚）
            return False
        op = "buy" if order.trade_type == TradeType.BUY else "sell"
        try:
            row = db.query(LiveOrder.id).filter(
                LiveOrder.live_session_id == self.session_id,
                LiveOrder.portfolio_strategy_id == order.portfolio_id,
                LiveOrder.strategy_id == order.strategy_id,
                LiveOrder.stock_code == order.stock_code,
                LiveOrder.bar_time == order.bar_time,
                LiveOrder.trade_type == op,
                LiveOrder.status.in_(["submitted", "partial"]),
            ).first()
        except Exception:  # noqa: BLE001
            db.rollback()
            logger.exception(
                "[pre-open] placed check failed, treat as placed (session %s)",
                self.session_id)
            return True
        return row is not None

    def _requeue_pre_open(self, db: Session, order: OrderEvent) -> None:
        """未报出的盘前单回 deferred 队列；DB 足迹缺失时按原 signal bar_time 重落行。

        保证「deferred 或 submitted 必居其一」：桥明确拒单/审批未过等路径里提交链
        已删除 deferred 行，若不重落，重启 recover 就会丢单；重落用原 signal
        bar_time（不是今天 09:1x），这样 recover 的 signal_bar_date 仍指向昨日、
        due 仍是今天，09:31 正常放行。
        """
        if getattr(order, "_deferred_row_id", None) is not None:
            return
        try:
            lo = self._persist_order_submitted(db, order, status="deferred")
            db.commit()
            order._deferred_row_id = lo.id
        except Exception:  # noqa: BLE001
            db.rollback()
            logger.exception(
                "[pre-open] re-persist deferred failed (session %s): %s %s",
                self.session_id, order.trade_type.value, order.stock_code)

    def _drop_pre_open_deferred_row(self, db: Session, order: OrderEvent) -> None:
        """拆单承接场景：删除仍残留的 deferred 行，防重启 recover 重复放行父单。"""
        row_id = getattr(order, "_deferred_row_id", None)
        if row_id is None:
            return
        try:
            db.query(LiveOrder).filter(LiveOrder.id == row_id).delete()
            db.commit()
        except Exception:  # noqa: BLE001
            db.rollback()
            logger.exception(
                "[pre-open] drop deferred row failed (session %s, row %s)",
                self.session_id, row_id)
            return
        order._deferred_row_id = None

    def _price_limit_bounds(self, stock_code: str):
        """当日涨跌停价 (prev_close, up, down)；通道不支持/无数据 → None（fail-open）。"""
        fn = getattr(self._dispatcher, "price_limit_bounds", None)
        if not callable(fn):
            return None
        try:
            return fn(stock_code)
        except Exception:  # noqa: BLE001
            logger.warning(
                "pre-open price limit fetch failed, skip clamp: %s",
                stock_code, exc_info=True)
            return None

    def _pre_open_price(self, order: OrderEvent) -> Optional[Decimal]:
        """盘前委托价：信号价 × (1 ± bias%)，再夹进当日涨跌停（各收 1 跳）。

        盘前没有开盘价（也不依赖 1 分钟 bar），基准取 deferred 单记录的 price
        = 信号 bar 收盘价（前一日），作为 09:31 路径「首根 bar 开盘价」的盘前代理；
        缺失时用通道昨收兜底，都拿不到就维持信号价原值。买 `min(基准×(1+bias),
        涨停−tick)`、卖 `max(基准×(1−bias), 跌停+tick)`；收 1 跳是为绕开本地
        涨跌停校验（买 ≥ 涨停 / 卖 ≤ 跌停一律拦），而不是把单挂到板上追价。
        """
        tick = tick_size(order.stock_code) or Decimal("0.01")
        bias = self._pre_open.price_bias_pct / Decimal("100")
        base = order.price if order.price is not None and order.price > 0 else None
        bounds = self._price_limit_bounds(order.stock_code)
        prev_close, limit_up, limit_down = bounds if bounds else (None, None, None)
        if base is None and prev_close:
            base = Decimal(str(prev_close))
        if base is None:
            return None
        if order.trade_type == TradeType.BUY:
            price = (base * (Decimal("1") + bias)).quantize(
                tick, rounding=ROUND_HALF_UP)
            if limit_up:
                price = min(price, Decimal(str(limit_up)) - tick)
        else:
            price = (base * (Decimal("1") - bias)).quantize(
                tick, rounding=ROUND_HALF_UP)
            if limit_down:
                price = max(price, Decimal(str(limit_down)) + tick)
        return price if price > 0 else None

    def _tick_external_signals(self) -> None:
        """外部信号注入轮（_tick_main 在 bar_poller.poll() 后调用）。

        ExternalSignalFileSource 时间窗口增量读取预警文件（时间戳游标 + lookback
        窗口——通达信启动补齐的历史信号在源内过滤，见 external_signal_source），
        逐条 _process_external_signal 注入；单条失败不阻断其余信号与主循环。
        源未启用（None）直接返回，行为不变。
        """
        if self._signal_source is None:
            return
        # 跨日重置（audit Fix1）：now_shanghai 日期变更 → reset_daily()（游标
        # 归位当前时间）+ 清空方向去重集合，同代码次日买卖两方向都可再触发。
        today = now_shanghai().date()
        if self._ext_sig_last_date != today:
            self._signal_source.reset_daily()
            self._ext_seen.clear()
            self._ext_sig_last_date = today
            logger.info(
                "external signal source reset for new day %s (session %s)",
                today, self.session_id)
        try:
            signals = self._signal_source.read_new_signals()
        except Exception:  # noqa: BLE001 — 读文件异常不阻断主循环
            logger.exception(
                "external signal read error (session %s)", self.session_id)
            return
        for sig in signals:
            try:
                self._process_external_signal(sig)
            except Exception:  # noqa: BLE001 — 单条信号失败不阻断其余信号
                logger.exception(
                    "external signal process error (session %s)", self.session_id)

    def _process_external_signal(self, sig: dict) -> None:
        """单条外部信号注入：公式名匹配策略 → 方向分类/去重 → 合成 bar 注入
        signal_cache → _handle_bar 全套（风控/审批/在途闸/去重与 bar 驱动同链）。

        方向口径（买卖双向放行）：
        - 买入类（OPEN/ADD）：沿用原开仓闸门（资金占比 / max_positions / 单仓
          金额 / 敞口 / 可用现金，一行不改）；满槽或已持仓仍跳过。
        - 卖出类（REDUCE/CLOSE）：不做仓位/资金检查，只受两条硬约束——无持仓
          不卖（不得卖空，本层跳过）与 T+1 实际可用不足拒单（提交链
          LiveT1Checker 强制，不绕过）。
        - 其它 signal_type：不可解析动作，丢弃 + 每 50 条节流告警，绝不猜
          方向、绝不默认成买入。

        去重键 = 同代码 + 同日 + 同方向（买卖各自认第一条，互不吞并）；同方向
        内重复推送不重复下单。买入/卖出方向由策略公式信号配置决定（预警文件
        8 列不含方向列）。

        预警文件 8 列没有数量/比例列：REDUCE 按清仓执行（signal_cache 以 CLOSE
        覆盖动作，日志 + risk 事件显式标注「未给出比例，按清仓执行」），不静默
        猜一个百分比；CLOSE 恒清仓。
        """
        source = self._signal_source
        if source is None:
            return
        symbol = normalize_external_symbol(sig.get("code"))
        if not symbol:
            logger.warning(
                "external signal invalid code %r (session %s)",
                sig.get("code"), self.session_id)
            return
        formula_name = sig.get("formula_name") or ""
        targets = [
            (port, ctx)
            for port in self.portfolios
            for ctx in port.strategies
            if ctx.formula_name and ctx.formula_name == formula_name
        ]
        if not targets:
            logger.info(
                "external signal dropped (no matching strategy): %s %s "
                "formula=%s (session %s)",
                symbol, sig.get("time"), formula_name, self.session_id)
            return
        # 逐目标策略按方向拆分公式信号配置（未知动作忽略 + 节流告警）
        plan = []
        has_buy = has_sell = False
        for port, ctx in targets:
            buy_cfgs, sell_cfgs = [], []
            for cfg in ctx.formula_signals:
                sig_type = cfg.get("signal_type")
                if sig_type in _BUY_SIGNAL_TYPES:
                    buy_cfgs.append(cfg)
                elif sig_type in _SELL_SIGNAL_TYPES:
                    sell_cfgs.append(cfg)
                else:
                    self._warn_unparseable_action(symbol, sig_type, formula_name)
            if buy_cfgs:
                has_buy = True
            if sell_cfgs:
                has_sell = True
            plan.append((port, ctx, buy_cfgs, sell_cfgs))
        # 方向级去重：同代码 + 同日 + 同方向各认第一条
        take_buy = has_buy and (symbol, "buy") not in self._ext_seen
        take_sell = has_sell and (symbol, "sell") not in self._ext_seen
        if not take_buy and not take_sell:
            logger.info(
                "external signal skip: %s %s direction(s) already consumed "
                "today (session %s)", symbol, sig.get("time"), self.session_id)
            return
        bar_time = sig["time"]
        price = sig["price"]
        reduce_clear_warned = set()  # 同一行同一 REDUCE 信号只标注一次
        injected_buy = injected_sell = False
        for port, ctx, buy_cfgs, sell_cfgs in plan:
            held_pos = ctx.positions.get(symbol)
            holding = held_pos is not None and held_pos.quantity > 0
            buy_allowed, sell_allowed = [], []
            if take_buy and buy_cfgs:
                # 买入类原闸门（一行不改）；槽位口径同 _submit_one_order：
                # 只数平台自己买入成交过的持仓，None 上限=该策略未配置 → 不限
                held_count = self._platform_held_count(ctx)
                pos_limit = self._slot_limit(port, ctx)
                if pos_limit is not None and held_count >= pos_limit:
                    logger.info(
                        "external signal skip: strategy %s max_positions full "
                        "(%d/%d) (session %s)",
                        ctx.strategy_id, held_count, pos_limit, self.session_id)
                elif holding:
                    logger.info(
                        "external signal skip: strategy %s already holding %s "
                        "(session %s)", ctx.strategy_id, symbol, self.session_id)
                else:
                    buy_allowed = buy_cfgs
            if take_sell and sell_cfgs:
                # 卖出类：不做仓位/资金检查；无持仓不卖（不得卖空）
                if holding:
                    sell_allowed = sell_cfgs
                else:
                    logger.info(
                        "external sell skip: strategy %s no position %s "
                        "(no short) (session %s)",
                        ctx.strategy_id, symbol, self.session_id)
            if not buy_allowed and not sell_allowed:
                continue
            # 「实际注入」才占本日方向名额：避免重复买入行把卖出名额一并吞掉
            if buy_allowed:
                injected_buy = True
            if sell_allowed:
                injected_sell = True
            outputs = []
            for cfg, is_sell in (
                [(cfg, False) for cfg in buy_allowed]
                + [(cfg, True) for cfg in sell_allowed]
            ):
                if is_sell and cfg.get("signal_type") == SignalType.REDUCE:
                    # 预警文件无比例列 → REDUCE 按清仓执行（CLOSE 覆盖动作）
                    outputs.append({
                        "name": cfg["signal_name"], "value": 1,
                        "signal_type": SignalType.CLOSE.value,
                    })
                    name = cfg["signal_name"]
                    if name not in reduce_clear_warned:
                        reduce_clear_warned.add(name)
                        msg = (
                            "外部信号 REDUCE 未给出减仓比例（预警文件无比例列），"
                            "按清仓执行：%s formula=%s signal=%s (session %s)"
                            % (symbol, formula_name, name, self.session_id)
                        )
                        logger.warning(msg)
                        self._emit("risk", {
                            "rule": "external_reduce_no_ratio",
                            "stock_code": symbol,
                            "message": msg,
                        })
                else:
                    outputs.append({"name": cfg["signal_name"], "value": 1})
            bar = BarEvent(
                stocks={symbol: {
                    "open": price, "high": price, "low": price,
                    "close": price, "volume": 0,
                }},
                bar_time=bar_time,
                period=ctx.period,
            )
            self.signal_cache[(ctx.strategy_id, symbol, bar_time)] = outputs
            self._handle_bar(port, bar)
        if injected_buy:
            self._ext_seen.add((symbol, "buy"))
        if injected_sell:
            self._ext_seen.add((symbol, "sell"))
        if not injected_buy and not injected_sell:
            logger.info(
                "external signal not injected (gate blocked): %s %s "
                "(session %s)", symbol, bar_time, self.session_id)
            return
        directions = "+".join(
            d for d, ok in (("buy", injected_buy), ("sell", injected_sell)) if ok)
        logger.info(
            "external signal inject: %s %s price=%s formula=%s directions=%s "
            "-> %d strategy instance(s) (session %s)",
            symbol, bar_time, price, formula_name, directions,
            len(targets), self.session_id)

    def _warn_unparseable_action(
        self, symbol: str, signal_type, formula_name: str
    ) -> None:
        """不可解析动作（未知 signal_type）告警：每 50 条 warning 一次。"""
        self._ext_unparseable_count += 1
        if (self._ext_unparseable_count - 1) % _UNPARSEABLE_WARN_EVERY == 0:
            logger.warning(
                "external signal unparseable action ignored: %s formula=%s "
                "signal_type=%r (warn #%d, every %d) (session %s)",
                symbol, formula_name, signal_type,
                self._ext_unparseable_count, _UNPARSEABLE_WARN_EVERY,
                self.session_id)

    def _sweep_timeout_orders(self, db: Session) -> None:
        """主循环超时收割段（P2 + A-2）：expire 兜底收割 + 超时撤单收割，同块提交。

        C2（终审）：expire 的 remark 兜底回填与 cancel 成功后的即时收割都会
        backfill/apply 持仓现金——与 deals 池 5s 回填真并发，整段（含 commit）
        持 _book_lock 串行化。commit 必须在锁内：后进锁的 deals 回填据此看到
        已提交的终态（filled/partial），不会对同一单重复 backfill/apply。

        批次1 3a：撤单收敛 canceled 后，对开启 requote_enabled 的策略做有限
        追价重下（重下走 _submit_one_order 同款提交链，仍持 _book_lock——
        pending_orders 增删须与 deals 池串行；锁序恒 book_lock → 通道锁，无反向）。
        """
        with self._book_lock:
            self._expire_stale_orders(db)
            # A-2（I-4/C-1）：在途单超时未成交 → 真实撤单（与 expire 同块提交；
            # 配置 cancel_unfilled_minutes<=0 或通道不支持撤单时内部直接跳过）。
            finalized = self._cancel_unfilled_orders(db)
            db.commit()
            # 批次1 3a：对刚收敛 canceled 的超时单做有限追价重下（次数上限兜底）
            if finalized:
                self._requote_after_timeout_cancel(db, finalized)
                db.commit()

    def _requote_after_timeout_cancel(self, db: Session, finalized: list) -> None:
        """批次1 3a：撤单收敛 canceled 后的有限追价重下（真机安全约束）。

        对本轮带撤单请求标记且收敛为 canceled 的单——两条收敛路径统一进本判定：
        主循环 _cancel_unfilled_orders 返回（委托表消失路径）+ deals 池
        _requote_after_cancel_converged 重查（53/54 终态同步路径，TDX/THS 当日
        委托表保留已撤单，真机主要走这条）；未带标记的单（GUI 手撤/收盘自动撤）
        不在两个入口的收集范围，维持现行为（终态 canceled），不重下：
        - 目标策略 requote_enabled 且 attempts < requote_max_attempts → 构造
          追价 OrderEvent 重下剩余量（quantity = 原量 − 已成交量，=0 不重下；
          price = 原价 ± tick_size，BUY +tick / SELL −tick；涨跌停界内不加校验，
          ENTER 后 THS/TDX 柜台校验兜底）；
        - 重下单 signal_bar_date 置 None（当 bar 走 _submit_one_order 同款提交链，
          不进 deferred 闸门）；
        - 次数记账 self._requote_attempts[(strategy_id, stock_code, signal_name,
          bar_date)]：重下单 attempts = 旧 n + 1（重启清零可接受——上限兜底）；
        - 非 requote 策略 / 超次数 → 维持现行为（终态 canceled），不重下。
        调用点持 _book_lock（pending_orders 增删与 deals 池串行）。
        """
        for lo in finalized:
            if lo.status != "canceled":
                continue  # 残余成交把单补齐（filled）等：非撤单终态不重下
            portfolio = next(
                (p for p in self.portfolios
                 if p.portfolio_id == lo.portfolio_strategy_id),
                None,
            )
            if portfolio is None:
                continue
            ctx = portfolio.find_strategy(lo.strategy_id)
            if ctx is None or not getattr(ctx, "requote_enabled", False):
                continue
            remaining = int(lo.quantity or 0) - int(lo.filled_quantity or 0)
            if remaining <= 0:
                continue  # 已全部成交（或零量）：无可追
            if lo.signal_type is None:
                # order_id() 需要 signal_type.value；历史 NULL 单跳过重下（保守）
                logger.warning(
                    "requote skip: order %s has NULL signal_type (legacy row)",
                    lo.id,
                )
                continue
            key = (
                lo.strategy_id, lo.stock_code, lo.signal_name or "",
                lo.bar_time.date() if lo.bar_time else None,
            )
            attempts = self._requote_attempts.get(key, 0)
            max_attempts = int(getattr(ctx, "requote_max_attempts", 2) or 0)
            if attempts >= max_attempts:
                logger.info(
                    "requote skip: order %s %s attempts %d >= max %d (session %s)",
                    lo.id, lo.stock_code, attempts, max_attempts, self.session_id,
                )
                continue
            # 防双触发（只触发一次）：同一收敛单可能同时出现在两条路径（消失 /
            # 53/54 终态）的输入里——按来源单 id 保证一单只判一次；另按
            # (strategy, code, signal, bar_date)+attempts 防同键同次数重复触发。
            fired_key = key + (attempts,)
            fired_order = ("order", lo.id)
            if fired_key in self._requote_fired or fired_order in self._requote_fired:
                logger.info(
                    "requote skip: order %s %s key=%s attempts=%d already fired "
                    "(dedup, session %s)",
                    lo.id, lo.stock_code, key, attempts, self.session_id,
                )
                continue
            step = tick_size(lo.stock_code)
            orig_price = Decimal(str(lo.price)) if lo.price is not None else None
            if orig_price is None:
                continue  # 无原价依据，不盲目追价
            price = (orig_price + step if (lo.trade_type or "").lower() == "buy"
                     else orig_price - step)
            requote = OrderEvent(
                strategy_id=lo.strategy_id,
                portfolio_id=lo.portfolio_strategy_id,
                stock_code=lo.stock_code,
                trade_type=TradeType(lo.trade_type.upper()),
                signal_type=SignalType(lo.signal_type),
                quantity=remaining,
                price=price,
                bar_time=lo.bar_time,
                signal_name=lo.signal_name or "",
                signal_bar_date=None,  # 追价单当 bar 提交，不进 deferred 闸门
            )
            self._requote_attempts[key] = attempts + 1
            self._requote_fired.add(fired_key)
            self._requote_fired.add(fired_order)
            self._rec_live(
                "requote", "submit", portfolio, requote,
                message="撤单追价重下：第 %d/%d 次，%s %s 追价后价格 %s、数量 %d"
                        % (attempts + 1, max_attempts, requote.stock_code,
                           requote.trade_type.value, price, remaining),
            )
            logger.info(
                "requote: order %s timeout-canceled → resubmit %s %s qty=%s "
                "price=%s attempt %d/%d (session %s)",
                lo.id, requote.trade_type.value, requote.stock_code,
                remaining, price, attempts + 1, max_attempts, self.session_id,
            )
            # 重下走与 _handle_bar 相同的提交闸门链（emit_signal=False：非新信号）。
            # _requote 标记豁免「同一根 bar 只下一次单」的持久幂等门：这是同一张单
            # 的加价重报，不是新决策（原单标记仍在，重报不写新标记）。
            requote._requote = True
            self._submit_one_order(db, portfolio, requote, ctx.period, emit_signal=False)
            self._drain_decisions(db, portfolio)

    # /deals 自适应轮询上限（秒）：单轮耗时超过当前间隔时退避，最慢 30s 一轮。
    _DEALS_POLL_MAX_SECONDS = 30.0

    async def _deals_loop(self) -> None:
        """G5：/deals 成交回报回填的独立轮询（自适应，下限 deals_poll_interval=5s）。

        成交秒级回报，独立短节拍让 LiveTrade/持仓/资金反馈近实时；主循环保持 60s 拉 bar。
        I-8（audit）：tick 投独立单 worker 池（_deals_executor）——主循环拉全池行情
        50-57s/轮时 deals 不再被饿死（原共享单池，deals tick 要排队等主 tick）。
        共享资源（TQ 连接）由 get_tdx_lock 串行化；I-8/C2（终审）：两池真并发，
        跨池记账（backfill/apply、pending_orders 增删）由引擎级 _book_lock 在调用点
        串行化（_tick_deals 持锁调 _poll_deals）。_poll_deals 内部已兜底桥离线/查失败。

        自适应节拍：下一轮间隔 = clamp(上一轮耗时, [5s, 30s])。掘金桥单次 /deals
        实测 10~13s（gm 持 GIL + 桥内全局锁串行），固定 5s 等于「上一轮还没结束
        又到期」的连续自压，也会放大桥侧客户端提前断连；快返回时自然回落 5s。
        """
        while self._running:
            started = time.monotonic()
            try:
                await asyncio.get_running_loop().run_in_executor(
                    self._deals_executor, self._tick_deals
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("deals loop executor error")
            await asyncio.sleep(self._deals_next_interval(time.monotonic() - started))

    def _deals_next_interval(self, elapsed: float) -> float:
        """_deals_loop 下一轮间隔：clamp(上一轮耗时, [deals_poll_interval, 30s])。

        快返回（elapsed ≤ 5s）→ 5s；慢一轮（如掘金桥 13s）→ 按实测耗时退避；
        上限 30s（低于主循环 60s，保证成交回填近实时）。
        """
        floor = self._deals_poll_interval
        ceiling = max(self._DEALS_POLL_MAX_SECONDS, floor)
        if elapsed is None or elapsed <= 0:
            return floor
        return min(ceiling, max(floor, elapsed))

    def _tick_deals(self) -> None:
        """_deals_loop 单轮同步体（在 worker 线程执行）：查未完结单 → 回填 /deals。

        异常在此捕获（非 CancelledError）——与原 _deals_loop 行为一致：记日志不外抛。
        C2（终审）：poll_deals 的回填（backfill → apply / pending 增删）与主池的超时
        收割、收盘清扫真并发，整段记账持 _book_lock 串行化。
        """
        try:
            # 快照采样（盘中 1m 数据源）：deals 节拍 5s、独立池不占主 worker；
            # 采样失败仅告警，不影响本轮回填主职责。滚动完成在 sample 内部
            # （门控关闭也执行），主循环 poll() 经适配器读取。
            if self._snapshot_synth is not None:
                try:
                    self._snapshot_synth.sample(now_shanghai())
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "snapshot synth sample error (session %s)", self.session_id)
            # 账户全貌快照采集（per-channel 节流）：落库 + 外部持仓导入 +
            # 真实可用资金缓存。失败仅告警——辅助链路不阻断回填主职责。
            if self._acct_snap_interval > 0:
                nowm = time.monotonic()
                if nowm - self._last_acct_capture >= self._acct_snap_interval:
                    self._last_acct_capture = nowm
                    db_cap = self._db_session_factory()
                    try:
                        self._capture_account_snapshot(db_cap)
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "account snapshot capture error (session %s)",
                            self.session_id)
                    finally:
                        db_cap.close()
            with self._book_lock:
                self._consume_execution_receipts()
                converged_ids = self._poll_deals() or []
                # 成交价偏离监控（P0-3）：本轮回填已落库，核对成交价 vs 审批价。
                # 必须在 _poll_deals 之后（否则读不到本轮新成交）。
                self._check_fill_price_deviation()
                # 批次2：拆单队列成交驱动续发——必须在 _poll_deals 之后：
                # 依赖本轮回填/终态同步把队首子单推进到 filled/canceled/rejected，
                # 再决定续发下一笔或整队中止。同持 _book_lock（RLock 可重入）。
                self._tick_split_children()
                # 撤单收敛统一 requote 挂点（53/54 终态同步路径）：带撤单请求
                # 标记的单在委托表呈 53/54 终态收敛 canceled 后做有限追价重下
                # （TDX/THS 当日委托表保留已撤单，真机超时撤单全走本路径）。
                # 持 _book_lock（重下走提交闸门链，pending_orders 增删须串行）。
                if converged_ids:
                    self._requote_after_cancel_converged(converged_ids)
                # 方案 E：成交回填可能已清仓 → 峰值表按「当前持仓」收敛（删已归零行）
                self._flush_position_peaks()
        except Exception:  # noqa: BLE001
            logger.exception("deals loop unexpected error")

    def _check_fill_price_deviation(self) -> None:
        """成交价偏离监控：回填成交价 vs 审批价超阈值 → risk/fill_price_deviation。

        为什么需要：QMT 桥在 ``price_type=opponent``（默认，prType=14）下按卖1/买1
        成交，``price``（审批价）对它没有约束力；跳空或盘口薄时成交价可能明显偏离
        审批价，而平台此前毫无感知（涨跌停前置校验也拦不住实际成交价）。

        范围：只对 ``supports_remark=True`` 的 HTTP 桥通道（QMT/掘金）生效——
        TDX 按 ``PRICE_MY`` 下发且提交前有 OCR 价格比对，THS 现网 ``latest`` 模式
        根本不键入价格（±让价空转），把它们纳入只会刷屏，不属本次范围。

        去重：键 (order_id, filled_quantity)，同一单增量成交只在量变化时复查一次；
        5s 节拍重复调用不会重复报同一笔。
        """
        limit = self._fill_price_deviation_pct
        if limit is None or limit <= 0:
            return
        if not getattr(self._dispatcher, "supports_remark", False):
            return
        db = self._db_session_factory()
        try:
            rows = (db.query(LiveOrder)
                      .filter(LiveOrder.live_session_id == self.session_id,
                              LiveOrder.filled_quantity > 0,
                              LiveOrder.filled_price.isnot(None),
                              LiveOrder.price.isnot(None))
                      .all())
            for lo in rows:
                key = (int(lo.id), int(lo.filled_quantity or 0))
                if key in self._fill_dev_checked:
                    continue
                self._fill_dev_checked.add(key)
                try:
                    approval = Decimal(str(lo.price))
                    fill = Decimal(str(lo.filled_price))
                except Exception:  # noqa: BLE001 脏值跳过，不阻断回填主流程
                    continue
                if approval <= 0 or fill <= 0:
                    continue
                dev = (fill - approval) / approval * Decimal("100")
                if abs(dev) <= limit:
                    continue
                msg = ("成交价偏离审批价 %.2f%% > 阈值 %.2f%%（审批 %s，成交 %s，%s %s）"
                       % (float(dev), float(limit), fmt_price(approval),
                          fmt_price(fill), (lo.trade_type or "").upper(),
                          lo.stock_code))
                logger.warning("fill price deviation: %s (session %s)",
                               msg, self.session_id)
                self._emit("risk", {
                    "session_id": self.session_id,
                    "rule": "fill_price_deviation",
                    "gate": "fill_price_deviation",
                    "order_id": lo.id,
                    "order_ref": lo.order_ref,
                    "portfolio_id": lo.portfolio_strategy_id,
                    "stock_code": lo.stock_code,
                    "trade_type": (lo.trade_type or "").upper(),
                    "approval_price": float(approval),
                    "fill_price": float(fill),
                    "deviation_pct": round(float(dev), 4),
                    "threshold_pct": float(limit),
                    "message": msg,
                })
        finally:
            db.close()

    # ---- 账户全貌对齐（快照采集 / 外部持仓导入 / 资金对齐）----

    def _read_account_overview(self):
        """统一读取真实账户：positions 三通道同形状（symbol/volume/available/
        cost/market_value），asset 有则附（TDX get_asset / THS get_account；
        QMT 桥缺资产端点 → 空 dict，现金对齐自动降级虚拟口径）。
        读取失败返回 None（本轮跳过）。"""
        d = self._dispatcher
        pos_fn = getattr(d, "get_positions", None)
        if not callable(pos_fn):
            pos_fn = getattr(d, "query_positions", None)
        if not callable(pos_fn):
            return None
        try:
            positions = pos_fn()
        except Exception:  # noqa: BLE001
            logger.warning(
                "account snapshot positions read failed (session %s)",
                self.session_id, exc_info=True)
            return None
        if positions is None:
            # 通道读失败（未登录/验证码/切页失败）≠ 真空仓：跳过本轮快照，
            # 否则会被当成 real=0 触发假对账差异（2026-11-06 实证）。
            logger.warning(
                "account snapshot positions unavailable (session %s): %s",
                self.session_id, getattr(d, "last_error", "") or "unknown")
            return None
        if not isinstance(positions, list):
            positions = []
        asset = {}
        asset_fn = getattr(d, "get_asset", None)
        if not callable(asset_fn):
            asset_fn = getattr(d, "get_account", None)
        if callable(asset_fn):
            try:
                raw = asset_fn()
                if isinstance(raw, dict):
                    asset = raw
                elif isinstance(raw, list) and raw and isinstance(raw[0], dict):
                    asset = raw[0]
            except Exception:  # noqa: BLE001
                logger.warning(
                    "account snapshot asset read failed (session %s)",
                    self.session_id, exc_info=True)
        return asset, positions

    def _note_cash_diff(self, real_cash: Decimal) -> None:
        """③ 柜台可用现金 vs 虚拟现金偏差告警（只告警，不改账）。

        期望值 = 各组合虚拟现金合计 − 在途买单冻结（柜台「可用」在采集时已扣
        冻结，见 _reserved_buy_cash 的 since 口径说明）。偏差 ≥ max(1000 元, 1%)
        视为账本与柜台失同步（漏记入金/出金、人工调账、成交未回填）→ 每会话
        10 分钟最多一条 risk 事件 + WARNING 日志。**绝不据此改虚拟现金**：
        改账只能走「外部现金流台账」或成交回填，保证可审计、可重放。
        """
        try:
            virtual = sum((p.account.cash for p in self.portfolios), Decimal("0"))
            expected = virtual - self._reserved_buy_cash()
            diff = Decimal(str(real_cash)) - expected
        except Exception:  # noqa: BLE001 — 观测失败不阻断采集主链路
            logger.debug("cash diff compute failed (session %s)", self.session_id,
                         exc_info=True)
            return
        base = abs(expected) if expected > 0 else Decimal("1")
        if abs(diff) < max(Decimal("1000"), base * Decimal("0.01")):
            return
        nowm = time.monotonic()
        if nowm - self._last_cash_diff_at < 600:
            return
        self._last_cash_diff_at = nowm
        logger.warning(
            "cash diff: 柜台可用 %s vs 虚拟现金合计 %s（扣在途冻结后 %s）偏差 %s "
            "(session %s) —— 仅告警，不改账",
            fmt_money(real_cash), fmt_money(virtual), fmt_money(expected),
            fmt_money(diff), self.session_id)
        self._emit("risk", {
            "gate": "cash_diff", "rule": "cash_diff", "action": "notice",
            "session_id": self.session_id,
            "real_cash": str(real_cash),
            "virtual_cash": str(virtual),
            "diff": str(diff),
            "message": ("柜台可用现金与虚拟现金不一致：柜台 %s / 虚拟 %s（扣在途冻结 "
                        "后 %s），偏差 %s。仅告警，未改账——如为入金/出金请登记"
                        "「现金流」台账" % (fmt_money(real_cash), fmt_money(virtual),
                                           fmt_money(expected), fmt_money(diff))),
        })

    # ---------------- 真实总资产 vs 虚拟净值（⑥） ----------------
    def virtual_equity(self) -> Decimal:
        """虚拟净值 = 各组合虚拟现金 + 持仓按最近已知价的市值（不含池外仓）。

        与 ``Portfolio.total_value`` 同口径（现金 + _holdings_value），供会话详情
        对比真实账户总资产。池外仓按设计不进虚拟账本（②），比对时从真实侧剔除。
        """
        total = Decimal("0")
        for p in self.portfolios:
            with self._book_lock:
                total += p.account.cash + p._holdings_value()
        return total

    def _out_of_pool_market_value(self, positions: list) -> Decimal:
        """真实持仓中不在任何组合股票池的市值合计（比对真实侧时剔除）。"""
        pools = self._pool_codes_by_portfolio or {}
        if not pools:
            return Decimal("0")
        all_codes: Set[str] = set()
        for codes in pools.values():
            all_codes.update(codes or ())
        total = Decimal("0")
        for r in positions or []:
            if not isinstance(r, dict):
                continue
            code = _position_code(r)
            if not code or code in all_codes:
                continue
            try:
                vol = int(r.get("volume") or 0)
            except (TypeError, ValueError, OverflowError):
                continue
            if vol <= 0:
                continue
            total += _position_market_value(r, vol, _position_cost(r, vol))
        return total

    def _note_equity_deviation(self, real_total: Decimal, positions: list) -> None:
        """⑥ 真实账户总资产 vs 虚拟净值比对；偏差 >20% 每会话只告警一次。

        真实侧剔除池外仓市值（池外按设计不进虚拟账本，②），否则有池外仓的
        会话会恒告警。只展示/告警，**不改任何账**：偏差是账本漏记（继承仓、
        未回填成交、外部调账）的信号，须人工核对后用「现金流台账」或重启
        recover 修正。
        """
        try:
            virtual = self.virtual_equity()
            out_value = self._out_of_pool_market_value(positions)
            expected = Decimal(real_total) - out_value
        except Exception:  # noqa: BLE001 — 观测失败不阻断采集主链路
            logger.debug("equity deviation compute failed (session %s)",
                         self.session_id, exc_info=True)
            return
        pct: Optional[Decimal] = None
        if virtual > 0:
            pct = (expected - virtual) / virtual
        self._latest_equity_check = {
            "real_total": str(real_total),
            "out_of_pool_value": str(out_value),
            "expected_total": str(expected),
            "virtual_total": str(virtual),
            "deviation_pct": str(pct) if pct is not None else None,
            "computed_at": now_shanghai().strftime("%Y-%m-%d %H:%M:%S"),
        }
        if self._equity_deviation_warned or pct is None:
            return
        if abs(pct) < Decimal("0.20"):
            return
        self._equity_deviation_warned = True
        logger.warning(
            "equity deviation: 真实总资产 %s（剔除池外 %s 后 %s）vs 虚拟净值 %s，"
            "偏差 %s (session %s) —— 账本可能与真实账户失同步，请人工核对",
            fmt_money(real_total), fmt_money(out_value), fmt_money(expected),
            fmt_money(virtual), "%+.2f%%" % (pct * 100), self.session_id)
        self._emit("risk", {
            "gate": "equity_deviation", "rule": "equity_deviation",
            "action": "notice", "session_id": self.session_id,
            "real_total": str(real_total),
            "virtual_total": str(virtual),
            "out_of_pool_value": str(out_value),
            "deviation_pct": str(pct),
            "message": ("真实账户总资产 %s（剔除池外 %s）与虚拟净值 %s 偏差 %+.2f%%，"
                        "请核对继承仓/成交回填/外部流水" % (
                            fmt_money(real_total), fmt_money(out_value),
                            fmt_money(virtual), pct * 100)),
        })

    # ---------------- 外部现金流（③） ----------------
    def apply_cash_flow(self, portfolio_id: int, amount: Decimal) -> bool:
        """把一笔外部现金流计入运行中的虚拟账本（现金与累计净流入同增）。

        只改内存态：DB 台账行由服务层写入，重启时 recover 从台账重放（幂等
        口径以台账为准）。净流入同增 → 「净值 − 净流入」不变，入金/出金不会
        被误判成当日盈亏或回撤（回测无外部现金流，净流入恒 0）。
        """
        port = next(
            (p for p in self.portfolios if p.portfolio_id == portfolio_id), None)
        if port is None:
            return False
        with self._book_lock:
            port.account.cash += amount
            port.external_net_inflow += amount
            cash = port.account.cash
            net = port.external_net_inflow
        logger.info(
            "cash flow: 组合 %s 外部现金流 %s → 虚拟现金 %s（累计净流入 %s）"
            " (session %s)",
            portfolio_id, fmt_money(amount), fmt_money(cash), fmt_money(net),
            self.session_id)
        self._emit("risk", {
            "gate": "cash_flow", "rule": "external_cash_flow", "action": "apply",
            "session_id": self.session_id, "portfolio_id": portfolio_id,
            "cash": str(cash), "net_inflow": str(net),
            "message": ("外部现金流 %s 已计入组合 %s 虚拟账本（净值/回撤/日亏按"
                        "剔除累计净流入口径）" % (fmt_money(amount), portfolio_id)),
        })
        return True

    def _capture_account_snapshot(self, db: Session) -> None:
        """采集真实账户全貌：落快照表 + 缓存真实可用资金 + 导入外部持仓 + 清理。

        同时持久化移动止损峰值（highest_price）：写入 = max(上次快照记录,
        当前虚拟账本峰值)；恢复 = 上次记录 > 账本峰值（重启丢峰）时还原——
        移动止损基准跨重启单调，不从启动价重新起算。"""
        # 读取前的下界时刻：读账户/持仓本身要花时间（桥 HTTP / THS 读表可达数秒），
        # 期间提交的单可能还没被柜台冻结进这份快照 → 在途预留去重只认「早于本时刻」
        # 提交的单（保守：读取期间提交的单仍计入预留）。
        read_at = now_shanghai()
        read = self._read_account_overview()
        if read is None:
            return
        asset, positions = read
        # 复用本轮真实持仓回读结果作为 live_service.engine_external_positions 的
        # 60s TTL 缓存：快照线程已每 60s 读一次真实持仓，/positions 请求不应再
        # 独立等一次 GUI 读表（THS 单次 6-10s，冷读会顶到前端 20s 超时）。
        try:
            self._external_pos_cache = (time.monotonic(), positions)
        except Exception:  # noqa: BLE001
            pass
        now = now_shanghai()
        # 上一份快照的峰值（跨重启恢复源）
        prev_peaks: Dict[str, Decimal] = {}
        last_snap = (
            db.query(LiveAccountSnapshot)
            .filter(LiveAccountSnapshot.live_session_id == self.session_id)
            .order_by(LiveAccountSnapshot.captured_at.desc(),
                      LiveAccountSnapshot.id.desc())
            .first()
        )
        if last_snap is not None:
            for p in (last_snap.positions or []):
                if p.highest_price is not None:
                    prev_peaks[p.stock_code] = Decimal(str(p.highest_price))
        # 本轮判定为「旧持仓残留峰值」的代码（先清仓再重开）：不还原、不写快照、
        # 也不交给导入路径（否则重启后会反复复活旧高点）
        stale_peak_codes: set = set()
        # 全部策略 ctx 的峰值查找表（虚拟账本当前值）
        ctx_peaks: Dict[str, Decimal] = {}
        for port in self.ctx.portfolios:
            for ctx in (getattr(port, "strategies", None) or []):
                for code, pos in (getattr(ctx, "positions", None) or {}).items():
                    hp = getattr(pos, "highest_price", None)
                    if hp and hp > ctx_peaks.get(code, Decimal("0")):
                        ctx_peaks[code] = hp
        snap = LiveAccountSnapshot(
            live_session_id=self.session_id,
            engine=self._engine_key or "unknown",
            captured_at=now,
            total_asset=_finite_or_none(asset.get("total_asset")),
            available_cash=_finite_or_none(asset.get("available")),
            market_value=_finite_or_none(asset.get("market_value")),
        )
        for r in positions:
            if not isinstance(r, dict):
                continue
            code = _position_code(r)
            prev_hp = prev_peaks.get(code)
            cur_hp = ctx_peaks.get(code)
            holders = []
            for port in self.ctx.portfolios:
                for ctx in (getattr(port, "strategies", None) or []):
                    p = (getattr(ctx, "positions", None) or {}).get(code)
                    if p is not None and p.quantity > 0:
                        holders.append(p)
            # 上一份快照的峰值只对「快照之前就存在」的持仓有效：若该代码当前
            # 全部持仓都是快照之后新开的（同代码先清仓再重开），旧峰值属于上
            # 一轮持仓——既不还原到持仓，也不写进新快照（否则会跨重启反复复活，
            # 把峰值钉在旧高位 → drawdown 立即 ≥ 阈值 → 误触全仓卖出）。
            # holders 为空（重启后尚未导入虚拟账本）→ 仍信旧峰值，由导入路径还原。
            snap_at = getattr(last_snap, "captured_at", None)
            if holders and prev_hp is not None and snap_at is not None:
                if not any(h.buy_time is None or h.buy_time <= snap_at for h in holders):
                    prev_hp = None
                    stale_peak_codes.add(code)
            effective_hp = max(
                (h for h in (prev_hp, cur_hp) if h is not None),
                default=None,
            )
            # QMT 会对停牌/异常持仓返回 inf/nan/DBL_MAX（实测 601989 市值 inf）：
            # 直接落库会让会话详情 JSON 序列化 500，这里先归一为 None。
            volume = _finite_or_none(r.get("volume"))
            avail_qty = _finite_or_none(r.get("available"))
            cost = _finite_or_none(r.get("cost"))
            mv = _finite_or_none(r.get("market_value"))
            # 峰值兜底（2026-11-07）：既无历史快照峰值、也无内存峰值（继承仓在
            # 行情轮询窗口外启动 / 该票无 bar → update_high(close) 从未执行）时，
            # 用柜台成本作起点。否则该列永远 NULL（tdx 会话 5 实证 1888 行全空）→
            # 重启后移动止损从当根 bar 收盘重新起算。取舍见导入路径注释。
            seed_hp = Decimal(str(cost)) if (cost is not None and cost > 0) else None
            effective_hp = max(
                (h for h in (effective_hp, seed_hp) if h is not None),
                default=None,
            )
            # 现价：优先组合最近已知价（与 total_value 估值同源），其次通道行内
            # 价格字段，最后回落成本。此前该列从未被赋值（全表 5701 行为 NULL），
            # 既无法区分「收盘后无现价」与「通道未提供价格」，也让快照不可读。
            cur_px = None
            for port in self.ctx.portfolios:
                px = (getattr(port, "_price_snapshot", None) or {}).get(code)
                if px is not None and px > 0:
                    cur_px = px
                    break
            if cur_px is None:
                for key in ("price", "current_price", "last_price", "now", "Price"):
                    v = _finite_or_none(r.get(key))
                    try:
                        if v is not None and float(v) > 0:
                            cur_px = Decimal(str(v))
                            break
                    except (TypeError, ValueError):
                        continue
            if cur_px is None and cost is not None and cost > 0:
                cur_px = Decimal(str(cost))
            snap.positions.append(LiveAccountPositionSnapshot(
                stock_code=code,
                quantity=int(volume or 0),
                available_quantity=int(avail_qty) if avail_qty is not None else None,
                cost_price=Decimal(str(cost)) if cost is not None else None,
                current_price=cur_px,
                # 市值缺失/为 0（通达信持仓明细无市值列）→ 复用 cost×volume 兜底，
                # 否则快照显示 0 会让工作台持仓市值合计凭空少一大块。
                # 传入已归一化的 mv（inf/nan 已被 _finite_or_none 转 None），
                # 避免 inf 绕过上面的序列化保护。
                market_value=_position_market_value(
                    {"market_value": mv}, int(volume or 0),
                    Decimal(str(cost)) if cost is not None else Decimal("0")) or None,
                highest_price=effective_hp,
            ))
            # 账本峰值低于历史记录（重启丢峰）→ 还原（只升不降语义安全）。
            # 两个口径修正（2026-11-07）：
            # ① 必须覆盖**所有**持有该代码的策略/组合——此前只还原第一个匹配持仓
            #    （break），多组合同代码时其余组合重启后峰值从当前价重新起算，
            #    移动止损静默失效（等于重启即免疫移动止损）；
            # ② 本轮持仓建仓时间晚于上一份快照 → 该快照峰值属于上一轮持仓
            #    （同代码先清仓再重开），不得复活旧高点。
            if effective_hp:
                for port in self.ctx.portfolios:
                    for ctx in (getattr(port, "strategies", None) or []):
                        p = (getattr(ctx, "positions", None) or {}).get(code)
                        if p is None or int(getattr(p, "quantity", 0) or 0) <= 0:
                            continue
                        if (snap_at is not None and p.buy_time is not None
                                and p.buy_time > snap_at):
                            continue
                        if effective_hp > (p.highest_price or Decimal("0")):
                            p.update_high(effective_hp)
                            # 方案 E：快照还原抬峰同样落库（只升不降）
                            self._note_peak_raised(
                                port.portfolio_id, code, effective_hp, now)
        db.add(snap)
        for _c in stale_peak_codes:
            prev_peaks.pop(_c, None)
        avail = _finite_or_none(asset.get("available"))
        if avail is not None:
            try:
                self._latest_account_cash = Decimal(str(avail))
                # 采集时刻取读取前下界（read_at），与 captured_at 同源（上海 naive）
                self._latest_account_cash_at = read_at
                # ③ 账本-柜台现金偏差观测（只告警不改账，10 分钟节流）
                self._note_cash_diff(self._latest_account_cash)
            except Exception:  # noqa: BLE001
                pass
        # 问题 6（2026-11-06 定稿）：先把桥 /positions 的数量/成本镜面覆盖到
        # 已存在的虚拟持仓（覆盖不累加），再导入账本缺失的代码。顺序不能反：
        # 先导入会把「已有仓」当新仓重复写入，先覆盖才能让对账差异归零
        # （归零路径是快照覆盖，不是再 apply 一次成交）。
        mirrored = self._mirror_bridge_positions(db, positions, now)
        imported = self._import_external_positions(db, positions, now, prev_peaks)
        # ⑥ 真实账户总资产 vs 虚拟净值（导入/镜面之后比对，避免继承仓尚未入账
        # 时产生一次性大偏差误告警；偏差只展示/告警一次，不改账）
        total_asset = _finite_or_none(asset.get("total_asset"))
        if total_asset is not None:
            try:
                self._note_equity_deviation(Decimal(str(total_asset)), positions)
            except Exception:  # noqa: BLE001 — 观测失败不阻断采集主链路
                logger.debug("equity deviation note failed (session %s)",
                             self.session_id, exc_info=True)
        # 人工干预（2026-11-11）：柜台整行消失 = 人工/外部清仓。连续确认后清理
        # 虚拟账本（否则幽灵持仓继续参与风控并产生必被拒的卖单），留审计行。
        try:
            self._detect_counter_cleared_holdings(db, positions, now)
        except Exception:  # noqa: BLE001 — 观测/修正失败不阻断采集主链路
            logger.exception(
                "detect counter cleared holdings failed (session %s)",
                self.session_id)
        # 对账面就地刷新：复用本轮已取到的真实持仓行（不再查 dispatcher），
        # 导入完成后立刻重算当期差异——否则 API 一直挂着启动时点（导入前）
        # 的旧照片（recover 对账先于导入，virtual=0/real=5300 会滞留一整天）。
        # 读取/计算失败 → 不调用替换（保留上一次差异与它原来的 computed_at）。
        diffs = None
        try:
            diffs = self._intraday_reconcile_diffs(db, positions)
            self._set_reconcile_mismatches(diffs, now)
        except Exception:  # noqa: BLE001
            logger.exception(
                "reconcile panel refresh failed (session %s)", self.session_id)
        # 盘中持仓对账（D3 同语义：仅告警不修正；节流 5 分钟）：虚拟聚合 vs
        # 快照真实持仓差异 → risk SSE 告警，把「收盘才发现的漂移」提前到盘中。
        # 复用上面已算好的 diffs，避免同一轮重复查询。
        nowm = time.monotonic()
        if nowm - self._last_intraday_reconcile >= _INTRADAY_RECONCILE_INTERVAL:
            self._last_intraday_reconcile = nowm
            try:
                if diffs is None:
                    diffs = self._intraday_reconcile_diffs(db, positions)
                for code, v, rr in diffs:
                    self._emit("risk", {
                        "session_id": self.session_id,
                        "rule": "reconcile_intraday",
                        "stock_code": code,
                        "virtual": v,
                        "real": rr,
                        "diff": rr - v,
                        "message": "盘中持仓对账差异 %s：虚拟=%d 真实=%d（仅告警，"
                                   "收盘对账口径一致）" % (code, v, rr),
                    })
            except Exception:  # noqa: BLE001
                logger.exception(
                    "intraday reconcile error (session %s)", self.session_id)
        # 保留 7 天
        db.query(LiveAccountSnapshot).filter(
            LiveAccountSnapshot.live_session_id == self.session_id,
            LiveAccountSnapshot.captured_at < now - timedelta(days=7),
        ).delete()
        # 方案 E：本轮还原/抬高的峰值随快照一并落库（清仓删行也在此收敛）
        self._flush_position_peaks(db)
        db.commit()
        if mirrored:
            logger.info(
                "bridge mirror applied to %d holding(s) (session %s)",
                mirrored, self.session_id)
        if imported:
            logger.info(
                "alignment import %d external position(s) (session %s)",
                imported, self.session_id)

    def _strategy_ctx_for_code(self, code: str):
        """外部持仓归属：股票池包含该代码的组合 → 其首个策略；否则
        首个组合的首个策略。返回 (portfolio, strategy_ctx) 或 None。"""
        pools = self._pool_codes_by_portfolio or {}
        for port in self.ctx.portfolios:
            if code in (pools.get(port.portfolio_id) or ()):
                strategies = getattr(port, "strategies", None) or []
                if strategies:
                    return port, strategies[0]
        for port in self.ctx.portfolios:
            strategies = getattr(port, "strategies", None) or []
            if strategies:
                return port, strategies[0]
        return None

    def _resolve_attribution(self, code: str):
        """外部成交归属（注入 OrderStateMachine）：code -> (portfolio, strategy) id 元组。

        与外部持仓导入同一口径，避免同一位代码在「持仓导入」与「成交补记」里
        落到不同策略造成虚拟现金/持仓错位。
        """
        target = self._strategy_ctx_for_code(code)
        if target is None:
            return None
        port, strategy_ctx = target
        return port.portfolio_id, strategy_ctx.strategy_id

    def _deduct_inherited_value(self, port, value: Decimal, source: str) -> None:
        """继承仓位的市值从虚拟现金中扣除（起账现金 = 该组合配置资金）。

        分账不分池（2026-11-11 P1-①）：每个组合虚拟现金起账 = 自身配置资金，与回测
        单组合资金口径一致。池内继承/外部增仓的市值是该配置资金已经变成的持仓，
        若只把数量镜像进虚拟账本而不扣现金，净值会被重复计入这部分市值 → 单仓金额、
        敞口上限、回撤分母等比放大。池外仓不进虚拟账本，不会走到这里（P1-②）。

        只在「真实数量 > 虚拟数量」的新增部分调用：平台自管买入/成交回填已经由
        apply_trade 扣过现金，不会重复扣。

        调用点需已持有 _book_lock（mirror 在锁内调用；import 路径单独加锁）。
        """
        if value <= 0:
            return
        port.account.deduct_cash(value)
        if port.account.cash < 0 and not self._neg_cash_warned:
            self._neg_cash_warned = True
            logger.warning(
                "inherited positions value %s exceeds cash baseline for portfolio %s "
                "(cash %s < 0, source=%s, session %s): 实盘资金基准应按"
                "「账户总资产（可用资金 + 持仓市值）」填写",
                value, port.portfolio_id, port.account.cash, source,
                self.session_id)

    def _credit_released_value(self, port, value: Decimal, source: str) -> None:
        """柜台侧减少的持仓按账面成本退回虚拟现金（``_deduct_inherited_value`` 的对称口径）。

        用于「人工/外部卖出动了平台账内的仓」：数量由镜面覆盖下调，但虚拟现金必须
        同步回补，否则虚拟净值凭空少掉这部分（假回撤，可能误触 max_drawdown 熔断）。
        按成本而非现价入账 → 盈亏中性，不制造假收益。调用点需已持有 ``_book_lock``。
        """
        if value <= 0:
            return
        port.account.add_cash(value)
        logger.info(
            "manual/external decrease: credit %s back to virtual cash "
            "(portfolio %s, source=%s, session %s)",
            value, port.portfolio_id, source, self.session_id)

    def _note_manual_intervention(
        self, db: Session, code: str, virtual_qty: int, real_qty: int,
        direction: str, reason: str, action: str = "notice",
        source: str = "bridge_mirror",
    ) -> None:
        """人工干预打标：柜台持仓出现「平台台账 + 在途单」解释不了的增减量。

        与既有标记的分工（关键区别）：
        - ``out_of_pool_position``：代码根本不在组合股票池，平台**从未**管过它；
        - ``external_deal``（live_orders.bridge_order_id=ext:*）：柜台成交无平台委托，
          只留痕不记账；
        - ``manual_intervention``（本方法）：代码**在**虚拟账本里，但柜台数量被人工/
          外部操作改动（手动加仓、手动减仓、手动清仓），是「人在柜台动了平台账内仓」
          的唯一标记。

        只打标 + 落审计（工作台决策流 + SSE），**不阻断交易、不改风控优先级**：
        数量/成本仍由镜面覆盖与 _import/_detect 路径按柜台口径修正。

        字段：code / virtual_qty / real_qty / diff / direction / reason 进 SSE 与
        决策事件 message；决策事件用既有列承载（param_value=差额、requested_qty=虚拟、
        final_qty=真实），无需新增表。
        """
        diff = int(real_qty) - int(virtual_qty)
        key = (code, int(virtual_qty), int(real_qty), action,
               now_shanghai().date())
        if key in self._manual_intervention_reported:
            return
        self._manual_intervention_reported.add(key)
        if len(self._manual_intervention_reported) > _EVENT_DEDUPE_MAX:
            self._manual_intervention_reported.clear()
            self._manual_intervention_reported.add(key)
        reason_text = {
            "counter_gt_ledger": "柜台多于虚拟台账（人工买入/外部转入）",
            "counter_lt_ledger": "柜台少于虚拟台账（人工卖出/转出）",
            "counter_position_gone": "柜台已无此持仓（人工清仓）",
            "close_reconcile_counter_zero": "收盘对账柜台已无此持仓，按柜台口径修正",
        }.get(reason, reason)
        direction_text = {"increase": "增加", "decrease": "减少"}.get(
            direction, direction)
        message = ("人工干预：%s 虚拟=%d 真实=%d 差额=%+d 方向=%s 原因=%s（%s）"
                   % (code, int(virtual_qty), int(real_qty), diff,
                      direction_text, reason_text, source))
        logger.warning(
            "manual intervention (session %s): %s virtual=%d real=%d diff=%+d "
            "direction=%s reason=%s action=%s source=%s",
            self.session_id, code, int(virtual_qty), int(real_qty), diff,
            direction, reason, action, source)
        self._emit("risk", {
            "session_id": self.session_id,
            "engine": self._engine_key,
            "rule": _MANUAL_INTERVENTION_GATE,
            "action": action,
            "stock_code": code,
            "virtual": int(virtual_qty),
            "real": int(real_qty),
            "diff": diff,
            "direction": direction,
            "reason": reason,
            "message": message,
        })
        try:
            db.add(LiveDecisionEvent(
                live_session_id=self.session_id,
                gate=_MANUAL_INTERVENTION_GATE,
                layer="live_gate",
                action=action,
                stock_code=code,
                param_name="diff_qty",
                param_value=float(diff),
                requested_qty=int(virtual_qty),
                final_qty=int(real_qty),
                message=message,
            ))
        except Exception:  # noqa: BLE001 — 观测链路失败不得影响记账
            logger.exception(
                "record manual intervention decision failed (session %s, code %s)",
                self.session_id, code)

    def _mirror_bridge_positions(self, db: Session, positions: list, now) -> int:
        """以桥 /positions 镜面覆盖虚拟账本的数量/可卖/成本（覆盖不累加）。

        口径（2026-11-06 定稿）：
        - 数量/可卖一律以柜台为准；成本用券商合并均价（``m_dOpenPrice``），
          作为止损/止盈/整仓盈亏的唯一基准。平台成交只更新 ``strategy_cost``
          归因，不再用它反推风控基准成本；
        - 在途单（DB submitted/partial）覆盖的代码跳过——提交到成交回填之间
          的持仓变动由成交回填负责，此时覆盖会与在途成交方向打架；
        - 桥没有对应行（已清仓）不强行清零：交给本地成交记录与对账收口
          （2026-11-11 起由 ``_detect_counter_cleared_holdings`` 连续确认后清仓 +
          审计，收盘再由 ``_apply_close_counter_correction`` 兜底），避免桥瞬时
          漏行把真实持仓抹掉；
        - 真实数量 > 虚拟数量（继承仓/外部手工增仓）的新增部分，按真实市值
          从虚拟现金扣除——资金基准是「账户总资产」口径，不扣会与基准双计；
        - 幂等：同一快照反复调用结果相同；只有实际变化才发 position 事件，
          避免 60s 节拍把同一持仓刷屏。
        """
        real: Dict[str, tuple] = {}
        for r in positions:
            if not isinstance(r, dict):
                continue
            code = _position_code(r)
            if not code:
                continue
            try:
                vol = int(r.get("volume") or 0)
            except (TypeError, ValueError, OverflowError):
                continue
            try:
                cost = _position_cost(r, vol)
            except Exception:  # noqa: BLE001
                cost = Decimal("0")
            avail = _finite_or_none(r.get("available"))
            real[code] = (
                max(0, vol), avail, cost,
                _position_market_value(r, vol, cost),
            )
        if not real:
            return 0
        inflight = {
            row[0] for row in db.query(LiveOrder.stock_code).filter(
                LiveOrder.live_session_id == self.session_id,
                LiveOrder.status.in_(["submitted", "partial"]),
            ).all()
        }
        query_date = now.date() if hasattr(now, "date") else now
        changed = 0
        with self._book_lock:
            for code, (vol, avail, cost, mv_total) in real.items():
                if code in inflight:
                    continue
                holders = []
                for port in self.ctx.portfolios:
                    for ctx in (getattr(port, "strategies", None) or []):
                        pos = (getattr(ctx, "positions", None) or {}).get(code)
                        if pos is not None and pos.quantity > 0:
                            holders.append((port, ctx, pos))
                if not holders:
                    continue  # 账本无此仓 → 由 _import_external_positions 导入
                # 人工干预（观测）：平台台账 + 在途单解释不了的数量差 → 打标 + 审计。
                # 此刻在途单覆盖的代码已被 inflight 跳过，平台自身成交已由
                # backfill/apply 记进账本，剩下的差异只可能来自柜台侧人工/外部操作。
                virt_total = sum(p.quantity for _, _, p in holders)
                if vol != virt_total:
                    self._note_manual_intervention(
                        db, code, virt_total, vol,
                        direction=("increase" if vol > virt_total else "decrease"),
                        reason=("counter_gt_ledger" if vol > virt_total
                                else "counter_lt_ledger"),
                        action="notice",
                        source="bridge_mirror",
                    )
                weights = [pos.quantity for _, _, pos in holders]
                qty_alloc = (_allocate_proportional(weights, vol)
                             if len(holders) > 1 else [vol])
                try:
                    sellable = vol if avail is None else max(0, min(int(avail), vol))
                except (TypeError, ValueError, OverflowError):
                    sellable = vol
                avail_alloc = (
                    qty_alloc if sellable >= vol
                    else _allocate_proportional(weights, sellable))
                for i, (port, ctx, pos) in enumerate(holders):
                    qty = qty_alloc[i]
                    sell = min(avail_alloc[i], qty)
                    before_qty, before_cost = pos.quantity, pos.avg_cost
                    pos.mirror_bridge(qty, sell, cost, query_date)
                    if cost <= 0:
                        # B5：柜台成本为负/未知 → avg_cost 已被 mirror_bridge 置 0
                        # （未知标记），止损/止盈阈值判断跳过（移动止损仍生效，
                        # 只看持仓期最高价）；一次性 risk 事件提示人工指定成本基准
                        # （同会话同票去重）。
                        self._note_cost_unknown(code, qty)
                    # 真实数量超出虚拟账本的部分 = 继承仓/外部手工增仓，其市值
                    # 已包含在资金基准（账户总资产）里 → 扣等额虚拟现金，避免双计。
                    # 例外（2026-11-10）：数量增但总成本未增 = 送转/拆股等股本变动
                    # （券商合并均价被摊薄），不是新买入 → **不扣现金**，否则虚拟
                    # 净值凭空少掉增量市值（假回撤 → 可能误触 max_drawdown 熔断）。
                    added = qty - before_qty
                    if added > 0 and mv_total > 0:
                        if _is_share_count_change(before_qty, before_cost, qty, cost):
                            if code not in self._share_change_noted:
                                self._share_change_noted.add(code)
                                logger.info(
                                    "bridge mirror: %s 数量 %d -> %d 且合并均价 %s -> %s"
                                    "（总成本未增）= 送转/拆股等股本变动，不扣虚拟现金"
                                    "（session %s）",
                                    code, before_qty, qty, before_cost, cost,
                                    self.session_id,
                                )
                        else:
                            self._deduct_inherited_value(
                                port,
                                mv_total / Decimal(vol) * Decimal(added),
                                "mirror",
                            )
                    removed = before_qty - qty
                    if removed > 0:
                        # 对称口径（2026-11-11）：柜台少掉的量 = 人工/外部卖出，
                        # 该部分市值按账面成本退回虚拟现金。不退回则虚拟净值凭空
                        # 少掉这部分（假回撤 → 可能误触 max_drawdown 熔断）。
                        # 用成本而非现价：账面按成本出仓，盈亏中性、不制造假收益。
                        self._credit_released_value(
                            port, before_cost * Decimal(removed), "mirror")
                    if pos.quantity == before_qty and pos.avg_cost == before_cost:
                        continue
                    changed += 1
                    logger.info(
                        "bridge mirror: %s %d -> %d (available=%d cost %s -> %s, "
                        "portfolio %s strategy %s, session %s)",
                        code, before_qty, pos.quantity, sell,
                        before_cost, pos.avg_cost, port.portfolio_id,
                        ctx.strategy_id, self.session_id)
                    self._emit("position", {
                        "portfolio_id": port.portfolio_id,
                        "strategy_id": ctx.strategy_id,
                        "stock_code": code,
                        "quantity": pos.quantity,
                        "avg_cost": float(dec_price(pos.avg_cost) or 0),
                        "market_value": float(dec_money(pos.market_value) or 0),
                        "pnl": 0,
                        "reason": "bridge_mirror",
                    })
        return changed

    def _detect_counter_cleared_holdings(
        self, db: Session, positions: list, now
    ) -> int:
        """柜台持仓行消失 → 连续确认后按「人工/外部清仓」清理虚拟账本并留审计。

        背景（2026-11-11）：用户在柜台把平台账内的仓手工卖光后，``/positions`` 里
        该代码整行消失（不是 0 股残行）。旧实现「桥没有对应行不强行清零」会让虚拟
        账本永久保留幽灵持仓：风控仍按它算止损/止盈并产生卖单（柜台拒单刷屏），
        收盘对账只告警不修正 → 幽灵仓跨日不消失。

        保守判据（缺一不可）：①本轮持仓读取成功（调用方已过滤读取失败）；②该代码
        不在本轮真实持仓；③无在途平台单；④**连续** ``_COUNTER_ABSENT_CONFIRM``
        次采集都缺（单次返回不完整不得清仓）。满足才清零虚拟持仓 + 按账面成本退回
        虚拟现金 + 落 ``manual_intervention`` 审计（action=apply）。

        幂等：清仓后 quantity=0，下一轮自然跳过；``_counter_absent_streak`` 同步清空。
        """
        real_codes = set()
        for r in positions:
            if not isinstance(r, dict):
                continue
            code = _position_code(r)
            if not code:
                continue
            try:
                vol = int(r.get("volume") or 0)
            except (TypeError, ValueError, OverflowError):
                continue
            if vol > 0:
                real_codes.add(code)
        inflight = {
            row[0] for row in db.query(LiveOrder.stock_code).filter(
                LiveOrder.live_session_id == self.session_id,
                LiveOrder.status.in_(["submitted", "partial"]),
            ).all()
        }
        query_date = now.date() if hasattr(now, "date") else now
        cleared = 0
        for port in self.ctx.portfolios:
            for ctx in (getattr(port, "strategies", None) or []):
                for code, pos in list((getattr(ctx, "positions", None) or {}).items()):
                    if pos is None or pos.quantity <= 0:
                        self._counter_absent_streak.pop(code, None)
                        continue
                    if code in real_codes:
                        self._counter_absent_streak.pop(code, None)
                        continue
                    if code in inflight:
                        continue
                    streak = self._counter_absent_streak.get(code, 0) + 1
                    self._counter_absent_streak[code] = streak
                    if streak < _COUNTER_ABSENT_CONFIRM:
                        logger.warning(
                            "counter position row missing, awaiting confirmation "
                            "%d/%d: %s (virtual=%d, session %s)",
                            streak, _COUNTER_ABSENT_CONFIRM, code,
                            pos.quantity, self.session_id)
                        continue
                    qty = pos.quantity
                    released = (pos.avg_cost or Decimal("0")) * Decimal(int(qty))
                    with self._book_lock:
                        self._credit_released_value(port, released, "counter_gone")
                        pos.mirror_bridge(0, 0, None, query_date)
                    self._counter_absent_streak.pop(code, None)
                    self._note_manual_intervention(
                        db, code, qty, 0, direction="decrease",
                        reason="counter_position_gone", action="apply",
                        source="counter_gone")
                    self._emit("position", {
                        "portfolio_id": port.portfolio_id,
                        "strategy_id": ctx.strategy_id,
                        "stock_code": code,
                        "quantity": 0,
                        "avg_cost": 0.0,
                        "market_value": 0.0,
                        "pnl": 0,
                        "reason": "manual_clear",
                    })
                    cleared += 1
        if cleared:
            logger.warning(
                "manual/external clear: %d holding(s) removed from virtual ledger "
                "because counter no longer reports them (session %s)",
                cleared, self.session_id)
        return cleared

    def _note_cost_unknown(self, code: str, volume: int) -> None:
        """柜台成本为负/未知：记一次性 WARN + risk 事件（同会话同代码只一条）。

        B5（2026-11-10）：cost<=0 不再用「市值÷数量」（=现价）冒充成本——
        那会让盈亏恒 0、风控形同失效且成本随价格每轮漂移。现在 Position.avg_cost
        保持 0（未知）。
        2026-11-11 文案校正：跳过的是**止损/止盈的阈值判断**（无成本基准）；
        **移动止损仍生效**——它只依赖「持仓期最高价」这条价格序列，不看成本
        （`risk_manager.check_trailing_stop`），是负成本/未知成本仓唯一的保护。
        展示侧同样按「成本未知」呈现，不显示漂移的现价。
        """
        if code in self._no_cost_warned:
            return
        self._no_cost_warned.add(code)
        logger.warning(
            "position %s x%d has negative/unknown broker cost — stop-loss / "
            "take-profit skipped (trailing-stop still active); set a cost basis "
            "manually; session %s",
            code, volume, self.session_id)
        self._emit("risk", {
            "rule": "position_cost_unknown",
            "stock_code": code,
            "quantity": int(volume),
            "session_id": self.session_id,
            "message": ("持仓 %s 柜台成本为负/未知（送转分红后常见），"
                        "该票不参与止损/止盈阈值判断，移动止损仍生效，"
                        "请人工指定成本基准"
                        % code),
        })

    def _import_external_positions(
        self, db: Session, positions: list, now,
        prev_peaks: Optional[Dict[str, Decimal]] = None,
    ) -> int:
        """真实持仓中虚拟账本缺失的代码 → 导入为对齐仓（挂主策略，纳入风控）。

        池内判定（2026-11-10）：代码属于某个组合股票池 → 仍按原逻辑归属并
        参与风控；**不属于任何池 → 不兜底归属**，只上报 out_of_pool_position，
        仓位留在展示层（engine_external_positions 的「外部持仓」行），不进
        虚拟账本（不占槽位、不参与风控、平台不代卖）。

        幂等：虚拟已有（quantity>0）→ 跳过；在途 BUY 覆盖的代码跳过
        （成交回填后自然进虚拟账本，重复导入会双计）。
        **池内**导入仓的市值同时从虚拟现金扣除——各组合虚拟现金起账 = 该组合配置资金
        （分账不分池，2026-11-11 P1-①），池内继承仓是「已用掉的配置资金」，不扣会与
        起账现金双计（净值虚高 → 单仓放大）。池外仓**不扣**：不进虚拟账本 → 不进虚拟
        现金/净值/回撤分母（P1-②），只在展示层单列「池外（不参与业绩）」。"""
        imported = 0
        imported_shares = 0
        deducted_value = Decimal("0")
        skipped_zero = 0
        no_code = 0
        out_of_pool = []  # [(code, volume)]：不在任何组合股票池
        no_attr = []  # [(code, volume)]：所有组合都没有策略 → 跳过且不受风控
        pools = self._pool_codes_by_portfolio or {}
        for r in positions:
            if not isinstance(r, dict):
                continue
            code = _position_code(r)
            if not code:
                # 换行/未知形状：以前在这里静默 continue（QMT 桥行只有
                # instrument+exchange，15 只持仓被全部丢掉且无任何日志）
                no_code += 1
                continue
            try:
                vol = int(r.get("volume") or 0)
            except (TypeError, ValueError, OverflowError):
                continue
            if vol <= 0:
                # 通达信/同花顺卖出后持仓表保留 0 股残行；不得导入成虚拟持仓
                skipped_zero += 1
                continue
            if pools and not any(
                    code in (pools.get(p.portfolio_id) or ())
                    for p in self.ctx.portfolios):
                # 柜台持仓不在任何组合股票池：**不做兜底归属**（2026-11-10）。
                # 只登记上报 + 展示层作为「外部持仓」行（engine_external_positions），
                # 不进任何组合策略的虚拟账本 → 不参与风控、不占槽位、不产生卖单；
                # 与 live_sell_scope=platform_only 同一口径。需要纳入策略时，
                # 把代码加入对应组合股票池，下一轮采集自动按池内仓导入。
                out_of_pool.append((code, vol))
                continue
            target = self._strategy_ctx_for_code(code)
            if target is None:
                # 所有已加载组合都没有策略：保持跳过语义（不进账本/不参与风控），
                # 但必须可见——否则真实仓位完全暴露于无保护状态且无任何痕迹
                no_attr.append((code, vol))
                continue
            port, ctx = target
            held = ctx.positions.get(code)
            if held is not None and held.quantity > 0:
                continue
            inflight = db.query(LiveOrder).filter(
                LiveOrder.live_session_id == self.session_id,
                LiveOrder.stock_code == code,
                LiveOrder.trade_type == "buy",
                LiveOrder.status.in_(["submitted", "partial"]),
            ).first()
            if inflight is not None:
                continue
            try:
                cost = _position_cost(r, vol)
            except Exception:  # noqa: BLE001
                cost = Decimal("0")
            if cost <= 0:
                self._note_cost_unknown(code, vol)
            pos = held if held is not None else Position(
                code, lot_size=self._engine._lot(code))
            # 与 _mirror_bridge_positions 同口径：数量/可卖/成本都以桥行镜像，
            # 不用 pos.buy（那会把 buy_time 记成今日 → T+1 当天不可卖）。
            try:
                avail = _finite_or_none(r.get("available"))
            except Exception:  # noqa: BLE001
                avail = None
            pos.mirror_bridge(vol, avail, cost,
                              now.date() if hasattr(now, "date") else now)
            # 继承仓市值从起账现金扣除：资金基准（live_capital / 回测初始资金）
            # 是「账户总资产」口径，继承仓本就包含在该基准里，不扣会双计
            inherited = _position_market_value(r, vol, cost)
            if inherited > 0:
                with self._book_lock:
                    self._deduct_inherited_value(port, inherited, "import")
                deducted_value += inherited
            # 峰值恢复：上次快照记录的历史最高价 > 成本 → 还原（移动止损基准
            # 跨重启不丢）。无历史记录时**用柜台成本兜底**（2026-11-07）：
            # 继承仓若在行情轮询窗口外启动/该票无 bar，`update_high(close)` 从未
            # 执行，峰值恒为 0 → 快照该列永远 NULL → 重启后移动止损从当根 bar
            # 收盘重新起算（= 重启即免疫）。用成本而非现价作起点：成本是已知的
            # 真实入场基准，从成本起算严格紧于从现价起算，且不依赖行情；代价是
            # 「已从真实历史高点回撤、但仍在成本×(1-阈值) 上方」的继承仓不会
            # 因历史高点触发（历史高点不可得，属可接受取舍）。
            prev_hp = (prev_peaks or {}).get(code)
            before_hp = pos.highest_price or Decimal("0")
            if prev_hp and prev_hp > 0:
                pos.update_high(prev_hp)
            elif cost > 0:
                pos.update_high(cost)
            # 方案 E：导入仓的起点峰值同样落库（只升不降），下次重启即可读表
            if (pos.highest_price or Decimal("0")) > before_hp:
                self._note_peak_raised(
                    port.portfolio_id, code, pos.highest_price, now)
            ctx.positions[code] = pos
            imported += 1
            imported_shares += vol
            # 动态扩池：导入仓纳入快照合成/行情轮询 → _check_risks 才能对其
            # 执行止损/止盈/移动止损（否则 stock_code not in bar.stocks 被跳过）
            if self._snapshot_synth is not None:
                try:
                    self._snapshot_synth.add_codes([code])
                except Exception:  # noqa: BLE001
                    logger.exception("synth add_codes error (session %s)", self.session_id)
            if self._bar_poller is not None and hasattr(self._bar_poller, "add_codes"):
                try:
                    self._bar_poller.add_codes([code])
                except Exception:  # noqa: BLE001
                    logger.exception("poller add_codes error (session %s)", self.session_id)
            self._emit("alignment_import", {
                "session_id": self.session_id,
                "stock_code": code,
                "quantity": vol,
                "cost_price": float(dec_price(cost) or 0),
                "portfolio_id": port.portfolio_id,
                "strategy_id": ctx.strategy_id,
                "message": "外部持仓导入：真实账户 %s ×%d（成本 %s）纳入 %d/%d 风控"
                           % (code, vol, fmt_price(cost), port.portfolio_id,
                              ctx.strategy_id),
            })
        # 外部持仓快照：本轮真实行中「未进入任何虚拟账本」的部分（池外仓、无策略
        # 组合、在途 BUY 覆盖）。只读展示用——不写入虚拟账本、不参与风控/槽位/卖出，
        # 也不影响本函数上面对池内仓的导入逻辑。
        virtual_codes = set()
        for p in self.ctx.portfolios:
            for ctx in p.strategies:
                for vcode, vpos in ctx.positions.items():
                    if getattr(vpos, "quantity", 0) > 0:
                        virtual_codes.add(vcode)
        snapshot = []
        for r in positions:
            if not isinstance(r, dict):
                continue
            code = _position_code(r)
            if not code or code in virtual_codes:
                continue
            try:
                vol = int(r.get("volume") or 0)
            except (TypeError, ValueError, OverflowError):
                continue
            if vol <= 0:
                continue
            try:
                cost = _position_cost(r, vol)
            except Exception:  # noqa: BLE001
                cost = Decimal("0")
            try:
                mv = _position_market_value(r, vol, cost)
            except Exception:  # noqa: BLE001
                mv = Decimal("0")
            snapshot.append({
                "symbol": code,
                "volume": vol,
                "available": _finite_or_none(r.get("available")),
                # 成本 <=0（送转后负数/柜台无效值）→ 0，前端显示「成本未知」
                "cost": float(cost) if cost and cost > 0 else 0.0,
                "market_value": float(mv) if mv and mv > 0 else 0.0,
            })
        self._external_positions = snapshot
        if skipped_zero:
            if skipped_zero != self._zero_skipped_reported:
                logger.info(
                    "alignment import skipped %d zero-quantity position row(s) "
                    "(session %s)", skipped_zero, self.session_id)
                self._zero_skipped_reported = skipped_zero
            else:
                # 同值重复（5s 采集节拍）降 DEBUG，避免 INFO 刷屏
                logger.debug(
                    "alignment import skipped %d zero-quantity position row(s) "
                    "(session %s)", skipped_zero, self.session_id)
        else:
            # 零股行消失 → 重置；下次再出现按「首次」报 INFO
            self._zero_skipped_reported = None
        # 导入成功：可辨识的汇总日志（排障时一眼确认账本是否真写入）
        if imported:
            logger.info(
                "import legacy positions: %d codes, total %d shares, "
                "inherited market value %s deducted from virtual cash (session %s)",
                imported, imported_shares, deducted_value, self.session_id)
        if no_code:
            if not self._no_code_rows_reported:
                logger.warning(
                    "import legacy positions: %d real position row(s) skipped, "
                    "no usable code (symbol / instrument+exchange both empty) "
                    "(session %s)", no_code, self.session_id)
                self._no_code_rows_reported = True
        else:
            self._no_code_rows_reported = False
        self._report_out_of_pool(db, out_of_pool)
        self._report_no_attributable(no_attr)
        return imported

    def _report_out_of_pool(self, db, rows: list) -> None:
        """池外持仓显式上报（2026-11-10 起：池外仓**不再兜底归属**）。

        代码不属任何组合股票池的外部仓只在展示层作为「外部持仓」行可见，
        不进虚拟账本 → 不参与风控、不占槽位、不产生卖单（与 platform_only
        同一口径）。需要纳入策略时，把代码加入对应组合股票池即可。
        同批代码集合去重，避免每轮采集（60s）重复入决策流刷屏。
        另落**一条汇总型**决策事件（gate=out_of_pool_position/action=notice）
        让工作台「决策/拦截」列表可见，每会话每日最多一条（逐票明细只走
        日志 + SSE risk，不逐票落表）。
        """
        codes = {c for c, _ in rows}
        if not rows:
            self._out_of_pool_reported = set()  # 恢复后可再次上报
            return
        if codes == self._out_of_pool_reported:
            return  # 同一批池外持仓已报过，去重防 5s 刷屏
        self._out_of_pool_reported = set(codes)
        for code, vol in rows:
            logger.warning(
                "alignment import: out-of-pool position (session %s) "
                "code=%s quantity=%d reason=不在任何组合股票池（不归属、不进风控）",
                self.session_id, code, vol)
        logger.warning(
            "alignment import: %d out-of-pool position(s) detected (session %s): %s",
            len(rows), self.session_id, ", ".join("%s×%d" % r for r in rows))
        quantities = {c: v for c, v in rows}
        detail = ", ".join("%s×%d" % r for r in rows)
        message = ("柜台持有但不在任何组合股票池：%s。这些为外部持仓，"
                   "不参与策略风控与槽位、平台不代卖（仅在持仓列表展示）；"
                   "如需纳入策略管理，请把代码加入对应组合股票池。" % detail)
        self._emit("risk", {
            "session_id": self.session_id,
            "engine": self._engine_key,
            "rule": "out_of_pool_position",
            "count": len(rows),
            "codes": sorted(codes),
            "quantities": quantities,
            "message": message,
        })
        self._note_out_of_pool_decision(db, rows, message)

    def _note_out_of_pool_decision(self, db, rows: list, message: str) -> None:
        """落一条汇总型决策事件（每会话每日最多一条，防 60s 轮询刷表）。

        逐票明细仍只走日志与 SSE risk；表里只有一条汇总（代码×数量清单在
        message 里）。**不做 200 字符截断**：该事件每会话每日一条，完整入库
        才不会把结论句「请把代码加入对应组合股票池」切掉（2026-11-10 实证
        9 只池外仓时 212 字符被切到「…请把」）。写库失败只记异常，不影响
        导入/交易主链路（观测链路）。
        """
        today = now_shanghai().date()
        if self._out_of_pool_event_day == today:
            return
        # 进程重启会让内存去重归零（今天 15:06 与 15:44 两次重启已实证重复落表），
        # 故以「当日已落库的行」为准：created_at 存 UTC，按上海日界换算。
        day_start_utc = datetime.combine(today, datetime.min.time()) - timedelta(hours=8)
        try:
            existing = (
                db.query(LiveDecisionEvent.id)
                .filter(
                    LiveDecisionEvent.live_session_id == self.session_id,
                    LiveDecisionEvent.gate == "out_of_pool_position",
                    LiveDecisionEvent.created_at >= day_start_utc,
                    LiveDecisionEvent.created_at < day_start_utc + timedelta(days=1),
                )
                .first()
            )
        except Exception:  # noqa: BLE001 查询失败不阻断写库
            existing = None
        if existing is not None:
            self._out_of_pool_event_day = today
            return
        self._out_of_pool_event_day = today
        payload = {
            "session_id": self.session_id,
            "portfolio_id": None,
            "strategy_id": None,
            "stock_code": None,
            "gate": "out_of_pool_position",
            "layer": "live_gate",
            "action": "notice",
            "param_name": "out_of_pool_count",
            "param_value": len(rows),
            "actual_value": None,
            "requested_qty": None,
            "final_qty": None,
            "message": message,
            "bar_time": None,
        }
        try:
            db.add(LiveDecisionEvent(
                live_session_id=self.session_id,
                gate=payload["gate"],
                layer=payload["layer"],
                action=payload["action"],
                param_name=payload["param_name"],
                param_value=payload["param_value"],
                message=payload["message"],
            ))
        except Exception:  # noqa: BLE001 — 观测链路失败不得影响导入
            logger.exception(
                "record out-of-pool decision event failed (session %s)",
                self.session_id)
            return
        self._emit("decision", payload)

    def _report_no_attributable(self, rows: list) -> None:
        """无任何可归属组合策略时的跳过上报（不改变导入行为，只让静默分支可见）。

        该分支仍 ``continue`` 跳过：仓位不进虚拟账本、不参与风控、不生成卖单，
        因此真实仓位处于「完全无风控保护」状态，必须显式告警 + SSE 事件。
        """
        codes = {c for c, _ in rows}
        if not rows:
            self._no_attr_reported = set()  # 配置恢复后可再次上报
            return
        if codes == self._no_attr_reported:
            return  # 同一批已报过，去重防 5s 刷屏
        self._no_attr_reported = set(codes)
        for code, vol in rows:
            logger.warning(
                "alignment import: no attributable portfolio (session %s) "
                "code=%s quantity=%d reason=会话无可归属组合策略，该仓位未纳入风控",
                self.session_id, code, vol)
        logger.warning(
            "alignment import: %d position(s) skipped without attributable "
            "portfolio (session %s): %s",
            len(rows), self.session_id, ", ".join("%s×%d" % r for r in rows))
        self._emit("risk", {
            "session_id": self.session_id,
            "engine": self._engine_key,
            "rule": "no_attributable_portfolio",
            "count": len(rows),
            "codes": sorted(codes),
            "quantities": {c: v for c, v in rows},
            "message": "会话无可归属组合策略：%s。该仓位完全不受风控保护，"
                       "请检查会话组合/策略配置（未导入虚拟账本，不参与风控）。"
                       % ", ".join("%s×%d" % r for r in rows),
        })

    def _on_bridge_slow(self, endpoint: str, elapsed: float,
                        summary: Optional[dict] = None) -> None:
        """桥单次调用超慢阈值告警（dispatcher 回调）。

        dispatcher 已按「会话」维度做窗口节流（每窗口最多一条，同一分钟内的多个
        端点合并）：``summary`` 非空表示上一窗口内还有被节流的慢请求，字段为
        ``{count, slowest_endpoint, slowest_elapsed_seconds, window_seconds}``。

        与「桥不可达/协议错误」区分：桥在线但响应慢（进程内查询排队），
        下单可能超时后转在途；只做观测告警，不改变交易行为。
        """
        summary = summary or {}
        prev_count = int(summary.get("count") or 0)
        message = "交易桥响应缓慢：%s 单次耗时 %.1fs" % (endpoint, elapsed)
        if prev_count:
            message += "；上一 %ss 窗口内另有 %d 次慢请求，最慢 %s %.1fs" % (
                int(float(summary.get("window_seconds") or 0)),
                prev_count,
                summary.get("slowest_endpoint") or endpoint,
                float(summary.get("slowest_elapsed_seconds") or 0.0),
            )
        message += ("。桥内查询排队，下单可能超时后转在途（等待委托回查认领），"
                    "与桥不可达/协议错误不同。")
        logger.warning(
            "bridge slow response: endpoint=%s elapsed=%.2fs (session %s)%s",
            endpoint, elapsed, self.session_id,
            " prev_window_count=%d" % prev_count if prev_count else "",
        )
        self._emit("risk", {
            "session_id": self.session_id,
            "engine": self._engine_key,
            "rule": "bridge_slow",
            "endpoint": endpoint,
            "elapsed_seconds": round(float(elapsed), 2),
            "window_slow_count": prev_count,
            "slowest_endpoint": summary.get("slowest_endpoint"),
            "slowest_elapsed_seconds": summary.get("slowest_elapsed_seconds"),
            "message": message,
        })

    def _volume_cap_shares(self, code: str) -> Optional[int]:
        """最近完成 1m bar 成交量（股）——快照合成源（TDX 快照 Volume 单位=手，
        ×100 转股，与回测 TQ K 线量纲对齐）。无合成器/无 bar → None。"""
        if self._snapshot_synth is None:
            return None
        try:
            bars = self._snapshot_synth.completed_bars([code], 2).get(code) or []
        except Exception:  # noqa: BLE001
            return None
        if not bars:
            return None
        v = bars[-1].get("volume")
        try:
            return int(v) * 100
        except (TypeError, ValueError):
            return None

    def _apply_volume_cap(self, order, portfolio: Portfolio) -> None:
        """成交量约束（实盘化，对齐回测 #2）：本单股数上限 = 最近完成 1m bar
        成交量 × volume_limit_ratio。

        - BUY > 上限且上限 ≥ 1 手 → 缩至整手（floor）；上限 < 1 手 → 拒单
          （标 _volume_capped_out，下一信号重试——回测「延后至量足 bar」的实盘化）；
        - SELL > 上限 → 缩量不取整手（A 股允许零股卖出）；上限 ≤ 0 → 放行
          （风控卖出优先，薄量由部分成交+回填/超时链收敛）；
        - 未启用 / 无快照量源 → 不干预（fail-open，iquant 通道量纲未标定）。
        """
        if not getattr(portfolio, "volume_limit_enabled", False):
            return
        ratio = getattr(portfolio, "volume_limit_ratio", None)
        if not ratio or ratio <= 0:
            return
        bar_vol = self._volume_cap_shares(order.stock_code)
        if not bar_vol or bar_vol <= 0:
            return
        cap_shares = int(ratio * Decimal(int(bar_vol)))
        if order.quantity <= cap_shares:
            return
        lot = self._engine._lot(order.stock_code)
        is_buy = order.trade_type.value == "BUY"
        final_qty = (cap_shares // lot) * lot if is_buy else cap_shares
        if final_qty >= lot or (not is_buy and final_qty > 0):
            self._rec_live(
                "volume_cap_shrink", "shrink", portfolio, order,
                message="成交量约束：订单 %d 股 > 本 bar 上限 %d 股（成交 %d 手 ×%s），缩量至 %d 股"
                        % (order.quantity, cap_shares, bar_vol, ratio, final_qty),
            )
            order.quantity = final_qty
        elif is_buy:
            order._volume_capped_out = True
            self._rec_live(
                "volume_cap", "block", portfolio, order,
                message="成交量约束：请求 %d 股，本 bar 上限 %d 股不足一手（成交 %d 手 ×%s），BUY 拒单待重试"
                        % (order.quantity, cap_shares, bar_vol, ratio),
            )

    def _intraday_reconcile_diffs(self, db: Session, positions: list) -> list:
        """盘中对账差异集：(code, 虚拟聚合量, 真实量)，仅差异项。

        在途单（DB submitted/partial）覆盖的代码跳过——提交到成交回填之间
        的瞬时差异不是漂移（查 DB 而非内存 _pending_orders：重启后内存空、
        DB 是耐久源）。导入对齐语义：导入后的仓已入虚拟账本，双方一致。"""
        virt: Dict[str, int] = {}
        for port in self.ctx.portfolios:
            for ctx in (getattr(port, "strategies", None) or []):
                for code, pos in (getattr(ctx, "positions", None) or {}).items():
                    if pos.quantity:
                        virt[code] = virt.get(code, 0) + pos.quantity
        real: Dict[str, int] = {}
        for r in positions:
            if not isinstance(r, dict):
                continue
            code = _position_code(r)
            if not code:
                continue
            try:
                v = int(r.get("volume") or 0)
            except (TypeError, ValueError, OverflowError):
                continue
            if v:
                real[code] = v
        inflight_codes = {
            row[0] for row in db.query(LiveOrder.stock_code).filter(
                LiveOrder.live_session_id == self.session_id,
                LiveOrder.status.in_(["submitted", "partial"]),
            ).all()
        }
        diffs = []
        for code in sorted(set(virt) | set(real)):
            if code in inflight_codes:
                continue
            v, rr = virt.get(code, 0), real.get(code, 0)
            if v != rr:
                diffs.append((code, v, rr))
        return diffs

    def _snapshot_latest_price(self, code: str) -> Optional[Tuple[Decimal, Optional[datetime]]]:
        """快照合成的最新样本价 → (价格, 采样分钟终点)；无样本/异常 → None。

        平台自己的盘中实时源（TQ 本地 1m 盘中只到上一交易日），供
        ``_refresh_submit_price`` 在 1m 行情陈旧时兜底。
        """
        synth = self._snapshot_synth
        if synth is None:
            return None
        try:
            latest = synth.latest_price(code)
        except Exception:  # noqa: BLE001
            return None
        if not latest:
            return None
        try:
            price = Decimal(str(latest[0]))
        except Exception:  # noqa: BLE001
            return None
        if price <= 0:
            return None
        return price, latest[1]

    def _warn_stale_quote(self, order, bar_time: Optional[datetime]) -> None:
        """1m 行情非当日 → 节流 WARNING（同 code 同一根 bar 只报一次，防批量放行刷屏）。"""
        raw = bar_time.isoformat() if bar_time is not None else "unknown"
        key = (str(order.stock_code), raw)
        if self._stale_quote_warned is None:
            self._stale_quote_warned = set()
        if key in self._stale_quote_warned:
            return
        self._stale_quote_warned.add(key)
        logger.warning(
            "提交前价格刷新：1m 行情非当日，不采用该价 code=%s bar_time=%s "
            "(session %s)", order.stock_code, raw, self.session_id,
        )

    def _refresh_submit_price(self, order) -> str:
        """提交前价格刷新：order.price 用**当日**实时价；返回价格来源标签。

        回测继承价 = 信号 bar 收盘价，跨日已过期（THS 审批价限价模式跳空
        必不成交）。取价顺序（2026-11-10 修正）：

        1. ``quote``：dispatcher 1m 行情最新 bar 的 close，**且该 bar 的时间
           必须是当日（Asia/Shanghai）**——通达信通道盘中本地 1m 只到上一
           交易日，拿昨收当实时价会把委托价报错（真机 300415 报 18.20，
           当时盘口 18.46 → 挂单不成交 → 超时撤单）；
        2. ``snapshot``：1m 缺失/陈旧 → 平台快照合成的最新样本价；
        3. ``signal`` / ``quote_stale_fallback``：两条路都取不到 → 保持
           order.price（信号价，不刷新）。

        价格来源写入日志与决策事件（gate=submit_price），便于事后核对
        委托价从哪来。
        """
        code = order.stock_code
        today = now_shanghai().date()
        original = order.price
        stale = False
        price: Optional[Decimal] = None
        source = "signal"
        try:
            bars = self._dispatcher.query_quote(code, period="1m", count=2)
        except Exception:  # noqa: BLE001
            bars = []
        if bars and isinstance(bars[-1], dict):
            last = bars[-1]
            try:
                quote_price = Decimal(str(last.get("close")))
            except Exception:  # noqa: BLE001
                quote_price = None
            if quote_price is not None and quote_price > 0:
                bar_time = parse_bar_time(last)
                if bar_time is not None and bar_time.date() == today:
                    price, source = quote_price, "quote"
                else:
                    # 时间戳缺失同样不可信：无从证明是当日价 → 不采用
                    stale = True
                    self._warn_stale_quote(order, bar_time)
        if price is None:
            snap = self._snapshot_latest_price(code)
            if snap is not None and (snap[1] is None or snap[1].date() == today):
                price, source = snap[0], "snapshot"
        if price is not None:
            order.price = price
        elif stale:
            source = "quote_stale_fallback"
        logger.info(
            "提交前价格刷新 code=%s price_source=%s price=%s signal_price=%s "
            "(session %s)", code, source, order.price, original, self.session_id,
        )
        self._recorder.record(
            gate="submit_price", layer="live_gate", action="refresh",
            portfolio_id=getattr(order, "portfolio_id", None),
            strategy_id=getattr(order, "strategy_id", None),
            stock_code=code, bar_time=order.bar_time,
            message="提交前价格刷新 price_source=%s price=%s signal_price=%s"
                    % (source, order.price, original),
        )
        return source

    def _day_first_bar_open(self, code: str) -> Optional[Decimal]:
        """当日首根 1m bar 的 open（None=取不到/无当日 bar）。

        P2-3：有快照合成器时直接用**内存里**的 ``open_at``（当日 09:30–09:31
        那根的 open）。旧实现一律 ``dispatcher.query_quote(1m, 250)``——tdx / ths /
        预警通道的本地 1m 盘中只到上一交易日 → 恒 None（放行价退化成实时价，
        偏离回测「次日开盘成交」口径），且每单多打一次 250 根 TQ 查询（会卡通达信）。
        只有没有合成器（iquant 桥直连 1m）时才回退旧查询。
        """
        synth = self._snapshot_synth
        if synth is not None:
            today = now_shanghai().date()
            start = datetime(today.year, today.month, today.day, 9, 30)
            try:
                opens = synth.open_at([code], start)
            except Exception:  # noqa: BLE001
                return None
            raw = (opens or {}).get(code)
            if raw is None:
                return None
            try:
                price = Decimal(str(raw))
            except Exception:  # noqa: BLE001
                return None
            return price if price > 0 else None
        try:
            bars = self._dispatcher.query_quote(code, period="1m", count=250)
        except Exception:  # noqa: BLE001
            return None
        today = now_shanghai().date()
        first_time = None
        first = None
        for row in bars or []:
            if not isinstance(row, dict):
                continue
            bar_time = parse_bar_time(row)
            if bar_time is None or bar_time.date() != today:
                continue
            if first_time is None or bar_time < first_time:
                first_time, first = bar_time, row
        if first is None:
            return None
        try:
            price = Decimal(str(first.get("open")))
        except Exception:  # noqa: BLE001
            return None
        return price if price > 0 else None

    def _clamp_release_price(self, order, price: Decimal) -> Decimal:
        """放行价夹进当日涨跌停（各收 1 跳）并 quantize 到最小价位。"""
        tick = tick_size(order.stock_code) or Decimal("0.01")
        bounds = self._price_limit_bounds(order.stock_code)
        _, limit_up, limit_down = bounds if bounds else (None, None, None)
        if limit_up is not None and limit_down is not None:
            up = Decimal(str(limit_up))
            down = Decimal(str(limit_down))
            if order.trade_type == TradeType.BUY:
                price = min(price, up - tick)
            else:
                price = max(price, down + tick)
        return price.quantize(tick, rounding=ROUND_HALF_UP)

    def _apply_minute_price_bias(self, portfolio: Portfolio, order: OrderEvent,
                                 ctx) -> None:
        """P1-5（2026-11-11）：分钟级单提交价在信号价上让 1 跳。

        问题：分钟级下单直接用 ``order.price``（信号 bar 收盘）做限价，价格逆势
        走开时首笔必挂（TDX 限价 / THS 键入审批价），只能等 180s 超时撤单后的
        ``_requote_after_timeout_cancel`` 再追——回测在下一根 bar 就成交了，实盘
        系统性少成交。本方法把「追价的一跳」前移到提交时：BUY +1 跳 / SELL −1 跳，
        仍夹当日涨跌停、quantize 到最小价位（复用 `_clamp_release_price`）。

        开关复用策略已有的 ``requote_enabled``（默认 False → 行为与旧版逐字一致）；
        未成交仍由既有追价路径兜底（`requote_max_attempts`、±1 跳）。日线及以上
        周期、deferred 放行价（`_release_priced`）、追价重下（±1 跳已加过）、
        盘前单（已按 bias% 让价）一律不动。
        """
        if not getattr(ctx, "requote_enabled", False):
            return
        if getattr(order, "_requote", False):
            return
        if getattr(order, "_release_priced", False):
            return
        # P1-6（2026-11-12）：重排（deferred_retry）复用同一个 OrderEvent 再次进入
        # 本方法，旧实现会再让 1 跳 → 首报 + 2 次重排最多偏 3 跳。让过价就打标，
        # 同一笔单只让一次。
        if getattr(order, "_price_biased", False):
            return
        if getattr(order, "_pre_open_bar_time", None) is not None:
            return
        period = self._order_period(portfolio, order)
        if period and period.lower() in _DAILY_PERIODS:
            return
        price = getattr(order, "price", None)
        if price is None or price <= 0:
            return
        if order.trade_type == TradeType.BUY:
            biased = price + tick_size(order.stock_code)
            direction = "上让"
        else:
            biased = price - tick_size(order.stock_code)
            direction = "下让"
        clamped = self._clamp_release_price(order, biased)
        if clamped is None or clamped == price:
            return
        order.price = clamped
        order._price_biased = True
        logger.info(
            "[submit-price] 分钟单%s 1 跳：signal=%s → submit=%s %s %s "
            "(requote_enabled, session %s)",
            direction, fmt_price(price), fmt_price(clamped),
            order.trade_type.value, order.stock_code, self.session_id)
        self._rec_live(
            "submit_price", "bias", portfolio, order,
            message=("分钟级提交价%s 1 跳（%s → %s，requote_enabled）："
                     "降低逆势挂单不成交，未成交仍走既有追价路径"
                     % (direction, fmt_price(price), fmt_price(clamped))),
        )

    def _refresh_release_price(self, order) -> str:
        """09:31 deferred 放行定价（P4，2026-11-11）：可复现优先（限价）。

        默认：限价 = **当日首根 1m bar 的开盘价**（夹当日涨跌停、quantize 到
        最小价位），按单下发 ``limit``——与回测「次日开盘成交」同价源，可复现。
        首根 open 取不到 → 退回 `_refresh_submit_price` 的当日实时 close，并记
        `gate=deferred_release / action=fallback` 风险事件（价格来源可审计）。

        `deferred_release_ensure_fill=True`（保证成交，默认关）→ 不覆盖价型，
        即通道默认对手价，优先成交。
        """
        # P1-5：放行价已按「首根 open / 实时 close」定好，不得再叠加分钟让价。
        order._release_priced = True
        if self._deferred_release_ensure_fill:
            source = self._refresh_submit_price(order)
            order._price_type_override = None
            return "ensure_fill:" + source
        open_price = self._day_first_bar_open(order.stock_code)
        if open_price is not None:
            order.price = self._clamp_release_price(order, open_price)
            order._price_type_override = self._pre_open_price_type()
            logger.info(
                "[deferred] release price = first 1m open %s (limit) code=%s "
                "(session %s)", fmt_price(order.price), order.stock_code,
                self.session_id)
            self._recorder.record(
                gate="deferred_release", layer="live_gate", action="release",
                portfolio_id=getattr(order, "portfolio_id", None),
                strategy_id=getattr(order, "strategy_id", None),
                stock_code=order.stock_code, bar_time=order.bar_time,
                message="放行价=当日首根 1m bar 开盘价 %s（限价，涨跌停内收 1 跳）"
                        % fmt_price(order.price),
            )
            return "first_bar_open"
        source = self._refresh_submit_price(order)
        order._price_type_override = self._pre_open_price_type()
        logger.warning(
            "[deferred] first 1m open unavailable, fallback to %s price=%s "
            "code=%s (session %s)", source, fmt_price(order.price),
            order.stock_code, self.session_id)
        self._emit_risk_safe({
            "session_id": self.session_id,
            "portfolio_id": getattr(order, "portfolio_id", None),
            "strategy_id": getattr(order, "strategy_id", None),
            "stock_code": order.stock_code,
            "trade_type": order.trade_type.value,
            "gate": "deferred_release",
            "action": "fallback",
            "rule": "first_bar_open_missing",
            "price": float(order.price),
            "price_source": source,
            "message": "当日首根 1m bar 开盘价不可得，退回实时价放行（非开盘价）",
        })
        return "quote_close_fallback:" + source

    def _requote_after_cancel_converged(self, order_ids: list) -> None:
        """53/54 终态同步收敛的撤单 requote 挂点（deals 池；持 _book_lock 调用）。

        poll_deals 的 db session 已在 finally 关闭，此处按 id 重查（只认本 session
        且已收敛 canceled 的行），复用 _requote_after_timeout_cancel 判定：
        requote_enabled / attempts 上限 / 剩余量扣减 / _requote_fired 防双触发。
        """
        if not order_ids:
            return
        db = self._db_session_factory()
        try:
            rows = (
                db.query(LiveOrder)
                .filter(
                    LiveOrder.id.in_([int(i) for i in order_ids]),
                    LiveOrder.live_session_id == self.session_id,
                    LiveOrder.status == "canceled",
                )
                .all()
            )
            if not rows:
                return
            self._requote_after_timeout_cancel(db, rows)
            db.commit()
        except Exception:  # noqa: BLE001
            db.rollback()
            raise
        finally:
            db.close()

    def _consume_execution_receipts(self) -> None:
        """执行回执消费（file 交接层执行器拒单/接单 → 工作台立即可见）。

        执行器回执经 ``dispatcher.execution_receipts()`` 弹出（单消费者=引擎，
        deals 轮询点、记账锁内），按 ``status`` 分流：

        - ``rejected``：按 order_id（=确定性 bridge_order_id）匹配在途 submitted 单
          → 走既有 rejected 终态（error_message=回执 error + order 事件，形状同
          expire 收口）；
        - ``accepted``（P0-1）：带 ``ref``（合同编号）与 ``final_qty``（通道钳量后的
          真实提交量）。若 ``final_qty`` 与引擎审批量不一致（THS 按 UI 实时可买/
          可卖静默钳量），**以真实数量为准**改写订单委托量并记 risk 事件
          ``channel_qty_clamped``；随后的成交回填按真实量判 filled 并更新持仓/虚拟
          现金（按成交成本），不再「引擎以为没买、实际已买」。

        order_id 缺失/匹配不上时回退按 entry 形状（stock_code+direction+quantity）
        匹配在途单；匹配不上 → 记 warning 丢弃（该单由 180s expire 兜底，不阻塞
        后续回执）；空回执零开销。通道可选能力：dispatcher 未实现
        execution_receipts（duck-typed fake/旧通道）视为无回执，直接跳过。
        """
        receipts_fn = getattr(self._dispatcher, "execution_receipts", None)
        receipts = receipts_fn() if callable(receipts_fn) else []
        if not receipts:
            return
        db = self._db_session_factory()
        try:
            pending = (
                db.query(LiveOrder)
                .filter(
                    LiveOrder.live_session_id == self.session_id,
                    LiveOrder.status.in_(["submitted", "partial"]),
                )
                .all()
            )
            for r in receipts:
                is_accept = str(r.get("status") or "").lower() == "accepted"
                lo = self._match_receipt_order(
                    pending, r, allow_claimed=is_accept)
                if lo is None:
                    logger.warning(
                        "execution receipt unmatched, dropped (expire fallback): "
                        "order_id=%s status=%s error=%s",
                        r.get("order_id"), r.get("status"), r.get("error"))
                    continue
                if is_accept:
                    self._apply_accept_receipt(lo, r)
                    continue
                lo.status = "rejected"
                lo.error_message = str(r.get("error") or "executor rejected")
                self._pending_orders.pop(lo.id, None)
                logger.warning(
                    "execution receipt: order %s %s %s rejected by executor (%s)",
                    lo.id, lo.trade_type, lo.stock_code, lo.error_message,
                )
                self._emit("order", {
                    "portfolio_id": lo.portfolio_strategy_id,
                    "order_id": lo.id,
                    "status": "rejected",
                    "stock_code": lo.stock_code,
                    "trade_type": (lo.trade_type or "").upper(),
                    "quantity": lo.quantity,
                    "bar_time": lo.bar_time.isoformat() if lo.bar_time else None,
                    "error_message": lo.error_message,
                })
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _apply_accept_receipt(self, lo: "LiveOrder", receipt: dict) -> None:
        """P0-1：消费执行器 accepted 回执——按通道真实提交量修正订单/账本。

        THS 会按 UI 实时「可买/可卖」静默钳量（引擎审批 1000 → 柜台实际 500），
        若不修正：legacy 认领数量不等 → 180s 判 rejected，但真实单已成交 →
        持仓账实不符且闸门释放后可能重复买入。此处把委托量改成 ``final_qty``，
        保持 submitted/partial 状态不变；成交回填（backfill_order 判
        filled_quantity >= quantity）随后的 apply 按真实量记持仓/虚拟现金。
        """
        ref = str(receipt.get("ref") or "")
        try:
            final_qty = int(receipt.get("final_qty") or 0)
        except (TypeError, ValueError):
            final_qty = 0
        engine_qty = int(lo.quantity or 0)
        if ref and not lo.order_ref:
            lo.order_ref = ref
        # 内存缓存（在途闸/风控读取的 _pending_orders）同步，避免下一轮 sync 前
        # 仍按审批量占用同股同向在途额度。
        cached = self._pending_orders.get(lo.id)
        if cached is not None and cached is not lo:
            if ref and not cached.order_ref:
                cached.order_ref = ref
            if final_qty > 0:
                try:
                    cached.quantity = final_qty
                except Exception:  # noqa: BLE001  detached/只读属性不阻断记账
                    pass
        if final_qty <= 0 or final_qty == engine_qty:
            return
        lo.quantity = final_qty
        logger.warning(
            "execution receipt: order %s %s %s quantity clamped by channel "
            "%s -> %s (using final quantity)",
            lo.id, lo.trade_type, lo.stock_code, engine_qty, final_qty)
        self._emit("risk", {
            "rule": "channel_qty_clamped",
            "session_id": self.session_id,
            "order_id": lo.id,
            "portfolio_id": lo.portfolio_strategy_id,
            "stock_code": lo.stock_code,
            "trade_type": (lo.trade_type or "").upper(),
            "engine_quantity": engine_qty,
            "final_quantity": final_qty,
            "message": "通道按可买/可卖钳量：审批 %d → 实际提交 %d，账本按实际量记账"
                       % (engine_qty, final_qty),
        })

    @staticmethod
    def _match_receipt_order(pending: list, receipt: dict,
                             allow_claimed: bool = False) -> Optional["LiveOrder"]:
        """回执 → 在途单匹配：order_id（=bridge_order_id）精确优先，
        entry 形状（stock_code+direction+quantity）回退。

        默认仅匹配 submitted 且无 order_ref 的单（rejected 回执→expire 同口径）；
        ``allow_claimed=True``（accepted 回执）时允许匹配已认领（order_ref 非空）
        或 partial 单——执行器接单回执可能晚于成交回填轮询到达。"""
        oid = str(receipt.get("order_id") or "")
        if oid:
            for lo in pending:
                if (lo.bridge_order_id == oid
                        and (allow_claimed
                             or (lo.status == "submitted"
                                 and lo.order_ref is None))):
                    return lo
        entry = receipt.get("entry") or {}
        code = str(entry.get("stock_code") or "")
        direction = str(entry.get("direction") or "").upper()
        volume = int(entry.get("quantity") or 0)
        if not code or not direction:
            return None
        for lo in pending:
            if not allow_claimed and (lo.status != "submitted"
                                      or lo.order_ref is not None):
                continue
            if str(lo.stock_code or "") != code:
                continue
            if (lo.trade_type or "").upper() != direction:
                continue
            if volume and int(lo.quantity or 0) != volume:
                continue
            return lo
        return None

    # ---------------- 日终/收盘时点编排（0010 步骤 5 转 DailyCloser）----------------
    # 14:30 日终估值、15:05 日线级别轮次（完整 bar）、15:05 收盘清扫的"时点判断 + 编排"已迁入
    # core.engine.live.daily_closer.DailyCloser。下列 _maybe_* 保留为薄委托（_tick_main
    # 与测试直连 engine._maybe_*(now=...) 穿透）；三个幂等日期标记经 property 委托到
    # DailyCloser，测试对 engine._last_daily_date 等的直接读写穿透到同一份状态。
    def _maybe_daily_close(self, now: Optional[datetime] = None) -> None:
        self._closer.maybe_daily_close(now)

    def _maybe_daily_bars(self, now: Optional[datetime] = None) -> None:
        self._closer.maybe_daily_bars(now)

    def _maybe_advance_daily_close(self, now: Optional[datetime] = None) -> None:
        """14:50 提前收盘价轮次（薄委托 → DailyCloser；仅 1d，会话级开关）。"""
        self._closer.maybe_advance_daily_close(now)

    def _maybe_close_sweep(self, now: Optional[datetime] = None) -> None:
        """15:05 收盘清扫（薄委托 → DailyCloser）。

        C2（终审）：清扫的回填/apply/终态收敛与 deals 池回填真并发，
        整段持 _book_lock 串行化（清扫内部含自己的 session 与 commit）。
        """
        with self._book_lock:
            self._closer.maybe_close_sweep(now)

    # ---------------- bar 驱动 ----------------
    def _on_bar(self, bar: BarEvent) -> None:
        """BarPoller 回调（1m 节拍）：① 驱动 1m 策略；② 按 bar stime 边界分发长周期。

        边界判定只读 1m bar stime（periods_on_boundary），不引入本机时钟；
        5m/15m/30m/1h 策略在边界时点才被驱动（C6(A)），1m 节拍不再每 bar 算长周期。
        C4(#28)：df_cache/raw_cache 建在此（跨组合共享）——同 (code,period) 只 query_quote
        一次、同 (code,period,formula) 只算一次（TQ 计算最贵）；同组 (formula,period) 的
        多 code 再合并成一次 compute_injected_batch（N set + 1 process，治 1m 全池节拍）。
        BarPoller 透传的本轮 bars（bar.bars_by_code）直接给 _handle_bar 注入复用——
        1m 判完成与算公式共用一次拉取，消除双拉（BarPoller 已拉 count=10，注入不再增量重拉）。
        """
        df_cache: Dict = {}
        raw_cache: Dict = {}
        bars_by_code = getattr(bar, "bars_by_code", None) or None
        is_baseline = bool(getattr(bar, "baseline", False))
        # P2-2：baseline 只拦「种子 bar 来自更早交易日」（离线残留 / iquant 首窗口
        # 只到上一交易日）。快照通道盘中启动时当日首根完成 bar（09:31）会被
        # BarPoller 当成基线 → 旧实现只放行 deferred、不跑公式与风控，等于每天
        # 稳定丢首分钟信号与止损。种子日 == 当日 → 按普通 bar 正常派发。
        if is_baseline and not self._is_stale_baseline(bar):
            bar = replace(bar, baseline=False)
            is_baseline = False
        handler = (
            self._handle_alert_bar
            if self.execution_mode == "alert" else self._handle_bar
        )
        for portfolio in self.portfolios:
            # 基线 bar 只用于放行 deferred（alert 模式无交易、无 deferred）→ 跳过
            if is_baseline and self.execution_mode == "alert":
                continue
            try:
                handler(
                    portfolio, bar, bars_by_code=bars_by_code,
                    df_cache=df_cache, raw_cache=raw_cache,
                )
            except BridgeUnavailableError as e:
                # 下单时桥离线：标记并中断本 bar（循环下一轮心跳会暂停）。
                # P2-6：走 _mark_bridge_offline 记录离线起点——下单路径发觉的断线
                # 未必伴随心跳失败（心跳在前、/order 在后），不记起点就丢缺口审计。
                self._mark_bridge_offline(now_shanghai())
                logger.warning("bridge unavailable on bar %s: %s", bar.bar_time, e)
                return
            except Exception:  # noqa: BLE001
                logger.exception(
                    "handle_bar error (portfolio %s, bar %s)",
                    portfolio.portfolio_id, bar.bar_time,
                )
        # C6(A)：1m bar 边界 → 分发 5m/15m/30m/1h（可累积）。
        # 基线 bar 不分发（窗口里可能是历史 bar，分发=用旧 bar 重放长周期策略）。
        if is_baseline:
            return
        # 先补偿此前被新鲜度闸拦下的边界（拿到当日 bar 才派发；超窗放弃）。
        try:
            self._retry_pending_boundaries(bar.bar_time)
        except BridgeUnavailableError as e:
            self._mark_bridge_offline(now_shanghai())  # P2-6：记录缺口起点
            logger.warning("bridge unavailable on pending boundary %s: %s", bar.bar_time, e)
            return
        if bar.bar_time not in self._dispatched_boundaries:
            dispatched_any = False
            stale_periods: Set[str] = set()
            for period in periods_on_boundary(bar.bar_time):
                try:
                    status = self._dispatch_period_bar(period, bar.bar_time)
                except BridgeUnavailableError as e:
                    self._mark_bridge_offline(now_shanghai())  # P2-6：记录缺口起点
                    logger.warning(
                        "bridge unavailable on %s boundary %s: %s", period, bar.bar_time, e
                    )
                    return
                except Exception:  # noqa: BLE001
                    logger.exception("dispatch %s boundary %s error", period, bar.bar_time)
                    continue
                if status == "dispatched":
                    dispatched_any = True
                elif status == "stale":
                    # 最新完成 bar 非当日（gate=period_bar_freshness）→ 本 boundary 不派发
                    stale_periods.add(period)
            # 只在成功派发后登记 boundary；整批被新鲜度闸拦下 → 挂待补偿，
            # 后续 1m bar 重试（不永久吞掉该 boundary，也绝不用旧 bar 触发）。
            if dispatched_any:
                self._dispatched_boundaries.add(bar.bar_time)
            if stale_periods:
                self._pending_boundaries.setdefault(bar.bar_time, set()).update(stale_periods)

    def _is_stale_baseline(self, bar: BarEvent) -> bool:
        """P2-2：baseline bar 是否「种子日早于当日」（只有这种才应跳过）。

        BarPoller 首次初始化 / 恢复时把窗口里最新一根标 baseline=True，本意是防
        「拿昨日旧 bar 重放信号」。但快照通道盘中启动时最新一根就是当日 09:31
        —— 那是当天真实的完成 bar，跳过它等于每天稳定丢一次首分钟公式信号与
        止损。故：种子日早于今天 → 仍是基线；当天 → 正常派发。
        """
        try:
            return bar.bar_time.date() < now_shanghai().date()
        except Exception:  # noqa: BLE001 — 时间异常时保守按基线处理（宁缺信号不重放旧 bar）
            return True

    # 非日内周期：bar_time 按约定是当日 00:00，只判交易日，不卡 09:30–15:00 时段。
    _DAILY_PERIODS = ("1d", "1w", "1mon")

    def _trading_allowed_for(self, bt: datetime, period: str) -> bool:
        """bar_time 是否允许下新单。日内周期判交易日+交易时段；日线及以上只判交易日。"""
        if period in self._DAILY_PERIODS:
            return self._calendar.is_trading_day(bt.date())
        return self._calendar.is_trading_allowed(bt)

    def _next_trading_day(self, d: date) -> date:
        """d 之后的下一个交易日（跨周末/节假日；日历退化时按工作日兜底）。"""
        cur = d + timedelta(days=1)
        for _ in range(31):
            if self._calendar.is_trading_day(cur):
                return cur
            cur += timedelta(days=1)
        return cur

    def _prev_trading_day(self, d: date) -> date:
        """d 之前的上一个交易日（跨周末/节假日；日历退化时按工作日兜底）。"""
        cur = d - timedelta(days=1)
        for _ in range(31):
            if self._calendar.is_trading_day(cur):
                return cur
            cur -= timedelta(days=1)
        return cur

    def _maybe_update_regime(self, portfolio: Portfolio, bar_date: date) -> None:
        """B5：每日首 bar 更新组合市场状态（纯因果——只吸收 ≤当日的基准收盘）。

        跳过条件：provider 未注入 / 组合无启用 regime 的策略（结果缓存于 ctx）/
        当日已更新 / 无 benchmark_code。provider 异常时告警并沿用旧标签（不中断 bar）。
        """
        if self._regime_provider is None:
            return
        if self._regime_updated.get(portfolio.portfolio_id) == bar_date:
            return
        enabled = getattr(portfolio, "_regime_any_enabled", None)
        if enabled is None:
            enabled = any(c.regime_filter_enabled for c in portfolio.strategies)
            portfolio._regime_any_enabled = enabled
        if not enabled:
            return
        code = self._benchmark_codes.get(portfolio.portfolio_id)
        if not code:
            return
        try:
            series = self._regime_provider(code, bar_date)  # {date: Decimal} 全量历史
            tracker = CausalRegimeTracker()
            tracker.feed_history(series)
            portfolio.current_regime = tracker.label
            self._regime_updated[portfolio.portfolio_id] = bar_date
        except Exception:
            logger.warning(
                "portfolio %s regime 更新失败（%s），沿用旧标签",
                portfolio.portfolio_id, code, exc_info=True,
            )

    def _tick_alert(self, now: Optional[datetime] = None) -> None:
        """alert 单轮：TQ 心跳 → 拉行情 → 调度信号；不碰委托/成交/持仓。"""
        was_online = self._bridge_online
        if not self._dispatcher.heartbeat():
            self._bridge_online = False
            return
        self._bridge_online = True
        if not was_online:
            # 离线恢复：丢弃离线期间积压的 bar/预热缓存，避免恢复首轮补发陈旧预警。
            if hasattr(self._bar_poller, "reset_baseline"):
                self._bar_poller.reset_baseline()
            if self._snapshot_synth is not None:
                self._snapshot_synth.reset()
            self._preheat_cache.clear()
        now = now or now_shanghai()
        # 行情轮询窗口（同 _tick_main）：收盘后/非交易日不拉行情；信号调度照常
        # （调度是本地时点判断，不依赖行情，收盘后仍可能触发提醒执行）。
        if self._quote_poll_active(now):
            if self._quote_poll_paused:
                self._quote_poll_paused = False
                self._leave_quote_poll_pause(now)
            self._bar_poller.poll()
        elif not self._quote_poll_paused:
            self._quote_poll_paused = True
            self._enter_quote_poll_pause(now)
        if self._signal_scheduler is not None:
            for batch in self._signal_scheduler.due(now):
                self._signal_scheduler.execute(batch)

    def _handle_alert_bar(
        self,
        portfolio,
        bar,
        bars_by_code=None,
        df_cache=None,
        raw_cache=None,
    ) -> None:
        """alert 模式：只算公式 + 监控信号，不下单、不跑风控。"""
        self.market_data.fill_signal_cache(
            portfolio,
            bar,
            bars_by_code=bars_by_code,
            df_cache=df_cache,
            raw_cache=raw_cache,
        )
        if self._signal_monitor is not None:
            self._signal_monitor.evaluate(
                portfolio,
                bar,
                self.market_data.signal_cache,
                strategies=portfolio.strategies,
                frequency=bar.period or "1m",
            )

    def _dedupe_inflight_sells(self, portfolio: Portfolio, orders) -> list:
        """B2（2026-11-10）：同票同向在途卖单去重（对称买入侧 inflight_opens）。

        ``_check_risks`` 每根 bar 全量重跑，上一轮的卖单还没回填（submitted/
        partial）时，同一只票会被重复生成止损/止盈/移动止损卖单——今天只靠
        T+1 可用量偶然拦住（同日第二次卖被拒），不是设计。这里在提交链之前
        按**未结单状态**（``_pending_orders`` 里的 submitted/partial SELL）判定：

        - 在途未成交数量 >= 本单目标数量 → 整单跳过，记一条
          ``gate=inflight_sell`` / ``skip`` 决策事件（同票同 bar 只记一条）；
        - 差额 > 0 → 按差额收窄本单数量继续提交（部分成交后剩余量仍会卖出），
          记 ``gate=inflight_sell`` / ``reduce``。
        同批多张同票卖单按序叠加占用，批内也不会重复。判据是「在途单」而非
        「当日卖过」——今天卖完后明天风控照常可以再卖。
        仅实盘路径（回测不走本方法），回测行为零变化。
        """
        if not orders:
            return orders
        inflight: Dict[tuple, int] = {}
        with self._book_lock:
            for lo in self._pending_orders.values():
                try:
                    if str(lo.trade_type).upper() != "SELL":
                        continue
                    if lo.portfolio_strategy_id != portfolio.portfolio_id:
                        continue
                    # 与 DB 同向门同口径（2026-11-11）：无法追溯（180s 起）或需人工
                    # 核对（grace 后）的单不再占用批内去重额度——否则止损卖单在
                    # 180s 释放后仍会被这里继续压住（最长到 600s），与统一口径矛盾。
                    err = str(getattr(lo, "error_message", "") or "")
                    if (err.startswith(_UNTRACEABLE_MARKER)
                            or err.startswith(_MANUAL_REVIEW_MARKER)):
                        continue
                    remaining = int(lo.quantity or 0) - int(lo.filled_quantity or 0)
                except DetachedInstanceError:
                    # 与 inflight_opens 同口径：属性不可读的单跳过，不打断整批
                    logger.warning(
                        "在途卖单属性不可读（detached+expired），inflight_sell 去重跳过该单")
                    continue
                if remaining <= 0:
                    continue
                key = (lo.strategy_id, lo.stock_code)
                inflight[key] = inflight.get(key, 0) + remaining
        if not inflight:
            return orders
        kept = []
        skip_recorded: set = set()
        for order in orders:
            if order.trade_type != TradeType.SELL:
                kept.append(order)
                continue
            key = (order.strategy_id, order.stock_code)
            reserved = inflight.get(key, 0)
            if reserved > 0:
                original = int(order.quantity or 0)
                remaining = original - reserved
                if remaining <= 0:
                    if key not in skip_recorded:
                        skip_recorded.add(key)
                        self._rec_live(
                            "inflight_sell", "skip", portfolio, order,
                            message="同票已有在途未结卖单 %d 股（submitted/partial），"
                                    "本次卖单跳过（防回填前重复卖出）" % reserved,
                        )
                    continue
                order.quantity = remaining
                self._rec_live(
                    "inflight_sell", "reduce", portfolio, order,
                    message="同票在途未结卖单 %d 股，本次卖单按差额收窄 %d → %d 股"
                            % (reserved, original, remaining),
                )
            kept.append(order)
            inflight[key] = reserved + int(order.quantity or 0)
        return kept

    def _note_valuation_gaps(self, portfolio: Portfolio, bar: BarEvent) -> None:
        """P1-8：零估值持仓（三级兜底全缺）→ WARNING + risk 事件，每 code 每日一次。

        ``Portfolio._holdings_value`` 已把「最后已知价/成本/前收都取不到」的代码
        收集到 ``_unpriced_codes``（这些仓按 0 计市值）。零估值不会中断交易，但
        会让组合净值偏低、回撤/日亏失真——必须让工作台能看见，不能静默。
        """
        codes = portfolio.unpriced_codes()
        if not codes:
            return
        day = bar.bar_time.date()
        if self._valuation_gap_date != day:
            self._valuation_gap_date = day
            self._valuation_gap_noted = set()
        for code in sorted(codes):
            if code in self._valuation_gap_noted:
                continue
            self._valuation_gap_noted.add(code)
            logger.warning(
                "零估值持仓 code=%s（无最后已知价/成本未知/无前收，市值按 0 计），"
                "组合 %s bar=%s (session %s)",
                code, portfolio.portfolio_id, bar.bar_time, self.session_id,
            )
            self._emit("risk", {
                "session_id": self.session_id,
                "portfolio_id": portfolio.portfolio_id,
                "stock_code": code,
                "gate": "valuation_missing_price",
                "action": "notice",
                "rule": "price_unavailable",
                "bar_time": bar.bar_time.strftime("%Y-%m-%d %H:%M:%S"),
                "message": "持仓无可用估值价（最后已知价/成本/前收全缺），市值按 0 计；"
                           "净值与回撤可能偏低",
            })

    def _handle_bar(
        self,
        portfolio: Portfolio,
        bar: BarEvent,
        bars_by_code: Optional[Dict[str, list]] = None,
        df_cache: Optional[Dict] = None,
        raw_cache: Optional[Dict] = None,
        mismatched_risk: Optional[bool] = None,
    ) -> None:
        """一根 bar：盯回撤 → 取信号/风控 → 先落 submitted → 发单 → 等回填。

        复用回测 Portfolio.on_bar（信号优先级/风控/主从/熔断全复用），
        通过 self._engine（已注入 HttpBridgeDispatcher + LiveT1Checker）做实盘下单。
        切片5 时序（G1）：先写 LiveOrder(status=submitted)+commit，再发 passorder——
        崩在 passorder 已发、未确认窗口时 DB 至少有 submitted 记录，供 _poll_deals 挂回回填。
        submitted 阶段不 apply_trade、不写 LiveTrade，真实成交回报由 _poll_deals 回填确认后 apply。
        E5/E6：每 bar 先调 update_peak（跨日刷新 prev_close + 更新峰值 + max_drawdown 熔断检测）。
        """
        # P1：记录分钟级 bar 的交易日（引擎级，多组合共享同一 bar 只推进一次），
        # 供 _release_signal_bar_deferred 判断「due 当日是否收到过行情」。
        self._note_minute_bar_date(bar)
        # 基线 bar（BarPoller 首次建基线/离线恢复首根）：只放行 deferred + 提交链，
        # 不跑公式信号评估与风控评估（防用历史 bar 重放），也不改风险状态。
        baseline = bool(getattr(bar, "baseline", False))
        # 错周期风控（会话三态解析结果）：None=未显式指定 → 用引擎级开关。
        # False 时 Portfolio.on_bar 对「非本策略周期」的 bar 只更新价格快照，
        # 不跑止损/止盈/移动止损（周线/月线同样只在自身周期边界评估）。
        if mismatched_risk is None:
            mismatched_risk = self._intraday_mismatched_risk
        # 盘前单开盘越价校验（P5，2026-11-11）：开盘后首根 1m bar 上检查盘前
        # 已报出单是否已被开盘价越过限价（必然未成交）→ 主动撤单。
        if not baseline:
            self._check_pre_open_orders_at_open(bar)
        # I-5（audit）：提交链入口拒单闸（deferred 放行与正常单共用此入口）——stop
        # 已请求后不再发起新单。竞态窗口：stop() cancel 任务后，worker 线程在途的
        # 最后一轮 tick 仍可能带着 bar 走进来。
        if self._stop_requested:
            return
        # trading_session（full/am/pm）组合时段闸（历史死配置首次接线）：非允许
        # 时段整轮跳过本组合（风控/信号/下单均不执行，与回测跳过 on_bar 同语义）。
        # 实盘按墙钟判定；时段约束仅对日内周期生效——日线及以上由 15:05 closer
        # 收盘落定轮次驱动（am 组合的日线策略若被闸过滤会永久卡死，session_allows_bar 周期判定）。
        # full=不筛（既有全局闸已管）。
        # 跳过时每日首次记决策事件（gate=trading_session、layer=portfolio）防刷屏。
        session = getattr(portfolio, "trading_session", None) or "full"
        if session in ("am", "pm") and not session_allows_bar(
            session, now_shanghai().time(), bar.period
        ):
            key = (portfolio.portfolio_id, bar.bar_time.date())
            if key not in self._session_skip_recorded:
                self._session_skip_recorded.add(key)
                self._recorder.record(
                    gate="trading_session", layer="portfolio", action="skip",
                    portfolio_id=portfolio.portfolio_id,
                    strategy_id=None, stock_code=None, bar_time=bar.bar_time,
                    message="交易时段闸：%s 时段组合当前 %s 不在允许窗口（%s），本轮跳过"
                            % (session, now_shanghai().strftime("%H:%M:%S"),
                               "09:30–11:30" if session == "am" else "13:00–15:00"),
                )
            db = self._db_session_factory()
            try:
                self._drain_decisions(db, portfolio)
            finally:
                db.close()
            return
        # E5：每 bar 更新峰值/回撤（分钟级 prev_close 只在跨日刷新，不误触 daily_loss）
        # 基线 bar 跳过：其 bar_time 可能是历史 bar（iquant 首窗口），改风险状态=
        # 用旧估值回写峰值/熔断计数。
        if not baseline:
            # ③ 风险口径净值 = 总市值 − 累计外部净流入：入金/出金/分红不改变回撤
            # 与日亏的分子分母（回测无外部现金流，净流入恒 0，口径一致）。
            # 仓位分配仍用 total_value（入金带来的真实购买力应当可用）。
            total_value = portfolio.risk_value(bar)
            # P1-8：三级兜底（最后已知价 → 成本 → 前收）全缺的持仓按 0 计市值，
            # 净值会偏低 → 记 risk 事件（每 code 每日一次），不让「假回撤」静默发生。
            self._note_valuation_gaps(portfolio, bar)
            # R3：每日首根 bar 补算上一日 daily_loss（与回测 enter_day 对齐，镜像回测时序——
            # 必须在 on_bar_update 之前调用：后者会把 _last_bar_date 置为当日，enter_day
            # 的跨日判断将失效）。日内亏损暂停/次日恢复在日首即生效，14:30 closer 的
            # update_daily 仍做当日收尾。首根 bar（无上一日数据）不调用，行为不变。
            if self._enter_day_updated.get(portfolio.portfolio_id) != bar.bar_time.date():
                portfolio.risk_manager.enter_day(
                    bar.bar_time.date(), portfolio.account.initial_capital
                )
                self._enter_day_updated[portfolio.portfolio_id] = bar.bar_time.date()
            # update_peak + max_drawdown 次日自动恢复检测 + H4 计数持久化归 BreakerService（0010 步骤 4）
            # P1-2：日线及以上策略的盘中分钟 bar 不抬峰（与回测「每日一次」对齐）
            self._breaker.on_bar_update(
                portfolio, total_value, bar.bar_time.date(),
                allow_peak_update=_allow_peak_update(portfolio, bar),
            )
        # F5：每 bar 刷一次桥可用持仓（多组合共享同一 bar 对象 → 强引用去重），
        # SELL 减仓上限用 m_nCanUseVolume（T+1 可用），供 cap_quantity 取数。
        if self._available_bar is not bar:
            self._available_bar = bar
            self._refresh_available_map()
        # 信号来源互斥：配置了外部信号文件（signal_file_path）时，公式信号只来自
        # 预警文件（_process_external_signal 注入），不再跑 TQ 公式计算——否则
        # TQ 路径会按其它 bar_time 另行出信号，与文件路径形成双触发。风控仍按本
        # bar 价照常执行（不依赖 signal_cache），回到「除信号来源外其余不变」。
        if self._signal_source is None and not baseline:
            # P2-7：TQ 全局锁等待超时 → 本轮公式信号缺失，但风控（止损/止盈/
            # 移动止损）不依赖 signal_cache，必须继续执行，不能因取数被占用而
            # 丢掉保护。超时记 WARNING + risk 事件（不静默）。
            try:
                self._fill_signal_cache(
                    portfolio, bar, bars_by_code=bars_by_code,
                    df_cache=df_cache, raw_cache=raw_cache,
                )
            except TDXLockTimeout as e:
                logger.warning(
                    "[tq-lock] formula injection skipped: %s (session %s, bar %s)",
                    e, self.session_id, bar.bar_time)
                self._emit("risk", {
                    "session_id": self.session_id,
                    "engine": self._engine_key,
                    "gate": "tq_lock_timeout",
                    "action": "skip",
                    "rule": "formula_injection",
                    "bar_time": (bar.bar_time.isoformat()
                                 if bar.bar_time else None),
                    "message": "通达信锁等待超时，本轮公式信号跳过（风控照常执行）",
                })
        if self._signal_monitor is not None and not baseline:
            self._signal_monitor.evaluate(
                portfolio,
                bar,
                self.signal_cache,
                strategies=portfolio.strategies,
                frequency=bar.period or "1m",
            )
        # C6：period 过滤——5m 边界 bar 不触发 1m 策略的风控单（_check_risks 读 bar.stocks close）
        # 在途单注入（OPEN 闸门 × max_positions）：_pending_orders 里的未成交 BUY
        # （submitted/partial）尚未落持仓（submitted 只建 quantity=0 的 Position），
        # 闸门 held 只数已成交——不注入则连续 bar 齐发 OPEN 超开/同票重复开仓。
        # partial 未 apply（只记账），整单仍在途，计入正确。
        # C2（终审）：跨池（deals 回填/超时收割）会并发 pop/替换 _pending_orders，
        # 遍历段持 _book_lock 防 dict changed size during iteration。
        inflight_opens: Dict[int, set] = {}
        with self._book_lock:
            for lo in self._pending_orders.values():
                try:
                    if lo.trade_type == "buy":
                        inflight_opens.setdefault(lo.strategy_id, set()).add(
                            lo.stock_code)
                except DetachedInstanceError:
                    # 兜底：在途集合应只存已加载对象（见 order_machine.detach_loaded），
                    # 万一残留 expired 实例，跳过该单而不是打断整根 bar 的信号评估。
                    logger.warning(
                        "在途单属性不可读（detached+expired），in-flight 注入跳过该单")
        # B5：每日首 bar 更新组合市场状态（未启用策略/provider 缺省 → 跳过，行为不变）
        if not baseline:
            self._maybe_update_regime(portfolio, bar.bar_time.date())
        # B-1 未来函数闸门放行：跨日后释放 deferred 的日线信号订单（bar_time 更新为
        # 当前 bar——决策时间戳语义），与 on_bar 新单合并重走同一提交链
        # （approve/收盘/日历/去重/在途门全复用，且放行单先行——对齐回测
        # 「上一 bar pending 先于本 bar 新信号成交」时序）。
        released = self._release_signal_bar_deferred(portfolio, bar)
        if baseline:
            # 基线 bar 不进入公式信号/风控评估（防重放），但 deferred 放行单照走
            # 批量资金分配与提交链。
            orders = list(released)
        else:
            orders = portfolio.on_bar(
                bar, signal_cache=self.signal_cache, period=bar.period,
                inflight_opens=inflight_opens or None,
                mismatched_risk=mismatched_risk,
            )
            if released:
                orders = list(released) + list(orders or [])
        # 先卖后买：SELL 全部先于 BUY 提交（风控卖出优先——释放现金和持仓槽位）。
        # SELL 提交后不等待成交（回填由 _tick_deals 5s 轮询处理），BUY 随即提交——
        # BUY 审批用虚拟现金，SELL 未回填前现金未释放，BUY 可能因资金不足被拒/缩量，
        # 属保守正确（宁可 BUY 失败，不可超卖）；等 SELL 成交再发 BUY 会阻塞主循环
        # 60s 节拍。稳定排序：SELL/BUY 各自组内保持原相对顺序（released 放行单
        # 先行语义在同类内保留）。
        if orders:
            # B2：同票同向在途卖单去重（_check_risks 每 bar 全量重跑，回填未完成时
            # 会重复生成止损/止盈卖单）。按「目标数量 − 在途未成交数量」收窄或跳过，
            # 仅实盘路径；回测不经过本方法。
            orders = self._dedupe_inflight_sells(portfolio, orders)
        if orders:
            orders.sort(key=lambda o: 0 if o.trade_type.value == "SELL" else 1)
            # 同批（同 bar/同组合）买单资金分配：先按目标金额比例分配并预留可用
            # 现金，再逐笔提交——避免 FIFO 先到先得让最后一笔在柜台因资金不足被拒。
            # deferred 单不参与（其可用资金以放行 bar 为准），单笔批次不分配。
            orders = self._allocate_batch_buys(
                portfolio, orders,
                post_close=bool(getattr(bar, "post_close", False)),
            )
        # db 会话在 on_bar 之后即开：决策闸门事件（熔断 halt/剥 BUY/风险触发）可能在无新单
        # 时也产生，需随本 bar 落库——故不再 orders 为空就早退，drain 放 finally 收尾。
        db = self._db_session_factory()
        # 本批提交失败的单（放行单需放回 deferred 队列重试，不能判 canceled）
        failed: List[object] = []
        try:
            for order in (orders or []):
                # 单笔订单走完整提交闸门链（撤单追价重下共用同一提交链，批次1 3a）。
                # 逐单隔离：单笔异常只回滚该笔事务并记录 ERROR，**不得终止同批其它单**
                # （2026-11-10 实证：一笔在途单属性不可读抛 DetachedInstanceError →
                # 整批中断，300286/300415 两笔 deferred 完全未提交）。
                try:
                    self._submit_one_order(
                        db, portfolio, order, bar.period,
                        post_close=bool(getattr(bar, "post_close", False)),
                    )
                except Exception:  # noqa: BLE001
                    db.rollback()
                    failed.append(order)
                    logger.exception(
                        "submit order failed (batch continues): %s %s %s qty=%s "
                        "price=%s bar=%s (session %s)",
                        order.trade_type.value, order.stock_code,
                        order.signal_name, order.quantity, order.price,
                        order.bar_time, self.session_id,
                    )
                    self._rec_live(
                        "submit_error", "reject", portfolio, order,
                        message="提交异常（单笔试错，不影响同批其它单），"
                                "放行单回队重试",
                    )
            # 放行单收尾：被闸门拦下（未提交）的放行单把 DB 行推到 canceled 终态，
            # 防止 deferred 行残留导致每次重启重复恢复（提交成功的单已在
            # _submit_one_order 内删旧行建 submitted 新行，不进入本分支）。
            # 提交异常的单不在此列——它们仍带 _deferred_row_id，须放回队列等下一根
            # bar 重试（判 canceled 会让该笔信号永久消失）。
            if released:
                self._restore_released_orders(
                    [o for o in released if any(o is f for f in failed)])
                self._settle_unsubmitted_released(
                    db, portfolio,
                    [o for o in released if not any(o is f for f in failed)])
            # 方案 E：本 bar 抬高的持仓峰值随本轮回合落库（只升不降；只写被抬高的行）
            if self._flush_position_peaks(db):
                db.commit()
            # 决策闸门事件随本 bar 统一落库 + SSE 推送（含 orders 为空时的 halt/strip 事件）。
            self._drain_decisions(db, portfolio)
        except Exception:
            db.rollback()
            # P3（2026-11-09）：放行即从 _signal_bar_deferred 摘除，若提交链抛异常，
            # 尚未真正提交的单必须放回队列（否则整笔单凭空消失，只能等自然过期）。
            self._restore_released_orders(released)
            raise
        finally:
            db.close()

    def _restore_released_orders(self, released) -> None:
        """放行单提交过程异常时回滚队列（幂等：已在队列/已提交的不重复放回）。"""
        if not released:
            return
        with self._book_lock:
            existing = {id(o) for o in self._signal_bar_deferred}
            for order in released:
                if id(order) in existing:
                    continue
                # _deferred_row_id 已被清空 = 单已真正提交（或已判过期），不回滚。
                if getattr(order, "_deferred_row_id", None) is None:
                    continue
                self._signal_bar_deferred.append(order)

    def _should_defer_order(
        self, portfolio: Portfolio, order: OrderEvent, post_close: bool = False
    ) -> bool:
        """B-1 未来函数闸门判定（提交链与批量资金分配共用同一口径）。

        日线级信号（signal_bar_date 非 None）在 signal_bar_date 当日及之前不提交；
        risk_timing=immediate（当日确认立即执行）跳过本闸门。post_close（15:05
        收盘落定轮次）无条件 defer——收盘后报单报不进交易所。
        """
        # P1（2026-11-09）：post_close 判据前置——收盘落定轮次恒 defer，不受
        # signal_bar_date 是否为空（外部信号注入 / 风控单也可能走到这里）影响。
        if post_close:
            return True
        if order.signal_bar_date is None or order.bar_time is None:
            return False
        ctx = portfolio.find_strategy(order.strategy_id)
        if ctx is None:
            return False
        return (order.bar_time.date() <= order.signal_bar_date
                and getattr(ctx, "risk_timing", "next_day") != "immediate")

    @staticmethod
    def _order_submit_at(lo) -> Optional[datetime]:
        """在途单提交时刻的**上界**估计（上海 naive）；不可知 → None。

        内存标记 ``_submit_at``（提交路径写入，微秒精度）优先；recover /
        sync_pending_orders 重建的在途单没有该标记，回退 DB ``created_at``
        （SQLite CURRENT_TIMESTAMP=UTC naive，**秒精度、向下截断**）＋8h ＋1s
        上界，与 recover 补记 ``_submit_times`` 的换算同口径。取上界是为了只在
        「确定早于快照采集时刻」时跳过预留，同一秒内的单按未反映处理（保守）。
        """
        at = getattr(lo, "_submit_at", None)
        if isinstance(at, datetime):
            return at
        created = getattr(lo, "created_at", None)
        if isinstance(created, datetime):
            return created + timedelta(hours=8, seconds=1)
        return None

    def _reserved_buy_cash(self, portfolio_id: Optional[int] = None,
                           since: Optional[datetime] = None) -> Decimal:
        """在途 BUY 已占用的资金预留（挂单价 × **未成交**量）。

        ``portfolio_id=None`` → 会话级合计（多组合共享同一资金账户时，真实可用
        额度必须按会话统一扣减，否则各组合各自吃满整账户）。

        ``since`` 非 None 时只计**提交时间晚于该时刻**的在途单：柜台账户快照的
        「可用」在采集时已扣掉此前提交单的冻结，再扣一次会把真实额度系统性低估
        （2026-11-10 001311 实证：56,115 → 5,155.30，3500 股被压到 300 股）。
        提交时间不可知的单按「未反映」计入（保守，宁可少买不超买）。

        预留从 ``_pending_orders`` 派生而非独立台账：全成交/拒单/撤单把单移出
        ``_pending_orders`` → 预留自动释放；recover 后 ``sync_pending_orders``
        天然重建。只预留**未成交量**（quantity − filled_quantity）：部分成交已由
        apply_filled_trade 增量扣减虚拟现金，再按整笔预留会把已花掉的部分二次占用，
        partial 窗口里可用现金被系统性低估。
        ``_pending_orders`` 由主池/deals 池跨线程增删，遍历必须在 ``_book_lock`` 内。
        """
        total = Decimal("0")
        with self._book_lock:
            for lo in self._pending_orders.values():
                try:
                    trade_type = (lo.trade_type or "").lower()
                    if trade_type != "buy":
                        continue
                    if lo.status not in ("submitted", "partial"):
                        continue
                    if portfolio_id is not None and \
                            lo.portfolio_strategy_id != portfolio_id:
                        continue
                    if since is not None:
                        submitted_at = self._order_submit_at(lo)
                        if submitted_at is not None and submitted_at <= since:
                            # 提交早于/等于快照采集时刻 → 其冻结已反映在可用里
                            continue
                    qty = (int(lo.quantity or 0)
                           - int(getattr(lo, "filled_quantity", 0) or 0))
                    if qty <= 0 or lo.price is None:
                        continue
                    total += Decimal(str(lo.price)) * qty
                except DetachedInstanceError:
                    # 兜底：在途集合应只存已加载对象（order_machine.detach_loaded）。
                    # 残留 expired 实例时跳过该单并告警——绝不能因预留口径把整批
                    # 提交打断（2026-11-10 实证：此处的 DetachedInstanceError 直接
                    # 让 300286/300415 两笔 deferred 未提交）。
                    logger.warning(
                        "在途单属性不可读（detached+expired），资金预留跳过该单")
        return total

    def _capital_share(self, portfolio: Portfolio) -> Decimal:
        """该组合配置资金占全会话配置资金的比重（真实账户缩量分配口径）。

        分账不分池（2026-11-11 P1-①）：各组合虚拟现金独立（起账 = 各自配置资金），
        真实账户只作**合计总闸**；真实可用不足时按该比重缩量分配，避免「先到的
        组合吃满、后到的组合饿死」。真实账户足以覆盖全部配置资金时，本比重不构成
        额外约束（各组合仍受自身账本现金限制）。
        """
        total = Decimal("0")
        for p in self.portfolios:
            try:
                cap = getattr(p, "initial_capital", None)
                if cap is None:
                    cap = p.account.initial_capital
                total += Decimal(str(cap))
            except Exception:  # noqa: BLE001
                continue
        if total <= 0:
            return Decimal("1")
        try:
            cap = getattr(portfolio, "initial_capital", None)
            if cap is None:
                cap = portfolio.account.initial_capital
            share = Decimal(str(cap)) / total
        except Exception:  # noqa: BLE001
            return Decimal("1")
        return share if share > 0 else Decimal("0")

    def _note_cash_share_scale(self, portfolio: Portfolio, real_cap: Decimal,
                               share: Decimal, shared_cap: Decimal) -> None:
        """真实账户可用不足、按配置资金占比缩量 → 每 (组合, 交易日) 记一条事件。"""
        today = now_shanghai().date()
        key = (portfolio.portfolio_id, today)
        if self._cash_share_scale_days.get(key) == today:
            return
        self._cash_share_scale_days[key] = today
        pct = (share * Decimal("100")).quantize(Decimal("0.01"))
        logger.warning(
            "真实账户可用 %s 不足，组合 %s 按配置资金占比 %s%% 缩量（上限 %s）",
            fmt_money(real_cap), portfolio.portfolio_id, pct, fmt_money(shared_cap))
        self._emit("risk", {
            "gate": "cash_share_scale",
            "rule": "real_account_insufficient",
            "action": "scale",
            "session_id": self.session_id,
            "portfolio_id": portfolio.portfolio_id,
            "param_value": str(pct),
            "final_qty": None,
            "message": ("真实账户可用 %s 不足覆盖全部组合配置资金，按配置资金占比 "
                        "%s%% 缩量分配（本组合本批上限 %s）"
                        % (fmt_money(real_cap), pct, fmt_money(shared_cap))),
        })

    def _buy_available_cash(self, portfolio: Portfolio) -> Decimal:
        """BUY 可用现金 = min(账本现金 − 本组合预留, 真实侧额度)。

        账本现金只在成交回填时扣减（submitted 阶段 apply=False），已提交未成交的
        BUY 不占账本余额，必须显式预留；真实可用快照在 bar 内可能滞后（5/60s
        刷新），不能反映本轮刚提交的单，故用「账本 − 预留」这条自记账口径兜住
        同 bar 内与跨 bar 的未成交占用。

        真实侧额度：多组合会话共享同一柜台资金账户，真实账户只作**合计总闸**
        （分账不分池，P1-①）——
          * 桥账户快照可用 → 扣「提交时间晚于快照 captured_at」的会话级在途预留；
            快照采集时间不可知时退回保守全额扣减（2026-11-10 001311 实证：快照
            可用本身已含采集前提交单的冻结，再全额扣一次会把额度系统性低估）；
          * 快照不可用但会话配置了 `live_capital`（真实账户总资产）→ 以它为兜底
            上限（×0.98 缓冲），同样扣会话级在途预留；
          * 两者都没有 → 只受账本现金约束（旧行为）。
        真实侧额度再按**该组合配置资金占全会话配置资金的比重**切分：真实账户
        缩水时各组合等比缩量（不按先到先得吃满），并记 `cash_share_scale` 事件。
        """
        avail = portfolio.account.cash - self._reserved_buy_cash(
            portfolio.portfolio_id)
        if avail < 0:
            avail = Decimal("0")
        real_cap: Optional[Decimal] = None
        if self._latest_account_cash is not None:
            if self._latest_account_cash_at is None:
                # 保守口径：不知道快照何时采的 → 全部在途预留都扣（宁可少买不超买）
                if not self._conservative_reserve_logged:
                    self._conservative_reserve_logged = True
                    logger.warning(
                        "资金闸门：账户快照采集时间不可知，真实侧在途预留按保守口径"
                        "全额扣减（宁可少买不超买，session %s）", self.session_id)
                reserved = self._reserved_buy_cash()
            else:
                reserved = self._reserved_buy_cash(since=self._latest_account_cash_at)
            real_cap = (self._latest_account_cash * Decimal("0.98")
                        - reserved).quantize(Decimal("0.01"))
        elif self._account_total_capital is not None:
            # 桥快照不可用 → 会话配置的账户总资产作真实侧兜底总闸
            real_cap = (self._account_total_capital * Decimal("0.98")
                        - self._reserved_buy_cash()).quantize(Decimal("0.01"))
        if real_cap is not None:
            if real_cap < 0:
                real_cap = Decimal("0")
            share = self._capital_share(portfolio)
            shared_cap = (real_cap * share).quantize(Decimal("0.01"))
            if shared_cap < avail:
                self._note_cash_share_scale(portfolio, real_cap, share, shared_cap)
                avail = shared_cap
        return avail

    def _allocate_batch_buys(
        self, portfolio: Portfolio, orders: List[OrderEvent],
        post_close: bool = False,
    ) -> List[OrderEvent]:
        """同 bar 同组合买单批量资金分配（先预留、再提交，避免 FIFO 饿死）。

        触发条件：本批可立即提交的 BUY 目标金额合计 > 可用现金。按目标金额比例
        分配可用现金，再按整手取整、贪心补手吃满零头；分配后不足 1 手的单剔除并
        记决策事件（不静默丢弃）。deferred 单不参与（其可用资金以放行 bar 为准）；
        单笔批次不分配，交给既有单笔审批（cap_quantity 逐单复核仍兜底）。

        可用现金口径见 ``_buy_available_cash``；在途预留从 ``_pending_orders`` 派生，
        成交回填令其收缩、拒单/撤单/全成交令其释放，故分配与逐单复核天然一致。
        """
        candidates = [
            o for o in (orders or [])
            if o.trade_type == TradeType.BUY
            and not self._should_defer_order(
                portfolio, o, post_close=post_close)
            and o.price is not None and o.price > 0
            and int(o.quantity or 0) > 0
        ]
        if len(candidates) < 2:
            return orders
        targets = [o.price * Decimal(int(o.quantity)) for o in candidates]
        total_target = sum(targets)
        avail = self._buy_available_cash(portfolio)
        if total_target <= avail:
            return orders
        lots = [self._engine._lot(o.stock_code) for o in candidates]
        # 1) 按目标金额比例分配 → 整手向下取整（不超过各单目标量）
        qty = []
        for o, tgt, lot in zip(candidates, targets, lots):
            raw = avail * tgt / total_target / o.price
            qty.append(min(int(raw / lot) * lot, int(o.quantity)))
        # 2) 贪心补手：比例取整的零头按整手回填（仍受各单目标量约束），
        #    避免"最后一笔"因取整余量而少买。仅回填已分到 ≥1 手的单——按比例
        #    不足 1 手的单直接跳过（不抢占别人的额度）。
        eligible = [q >= lot for q, lot in zip(qty, lots)]
        leftover = avail - sum(Decimal(q) * o.price for q, o in zip(qty, candidates))
        progressed = True
        while progressed:
            progressed = False
            for i, o in enumerate(candidates):
                if not eligible[i]:
                    continue
                nxt = qty[i] + lots[i]
                if nxt > int(o.quantity):
                    continue
                need = o.price * lots[i]
                if leftover >= need:
                    qty[i] = nxt
                    leftover -= need
                    progressed = True
        skipped = set()
        for o, q, lot in zip(candidates, qty, lots):
            original = int(o.quantity)
            if q < lot:
                skipped.add(id(o))
                self._rec_live(
                    "batch_cash_alloc", "block", portfolio, o,
                    message="同批资金分配后不足 1 手（目标 %d 股 @%s，本批可用 %s），跳过"
                            % (original, fmt_price(o.price), fmt_money(avail)),
                )
                continue
            if q < original:
                self._rec_live(
                    "batch_cash_alloc", "shrink", portfolio, o,
                    message="同批资金分配缩量：目标 %d 股 → %d 股（本批可用 %s，"
                            "同批目标合计 %s）"
                            % (original, q, fmt_money(avail), fmt_money(total_target)),
                )
            o.quantity = q
        return [o for o in orders if id(o) not in skipped]

    @staticmethod
    def _platform_held_count(ctx) -> int:
        """槽位闸的持仓口径（方案 A）：只数「平台自己买入成交过」的持仓。

        判据 ``Position.platform_quantity > 0``——该字段只由平台成交累加
        （apply_trade，成交回填 / recover 重放同一入口），外部/手工导入仓
        （快照 import、mirror 新建）与外部成交补记（note_strategy_trade）都不写，
        故恒为 0 → 不占名额（如 510050 手工仓）。

        不用 DB ``live_trades`` 判据：该表没有「持有期」概念，某代码历史上的
        平台买入会永久占名额（平台清仓后人工再买入仍被当成平台仓）；且外部
        成交补记行（order_type=external）也在表里，裸“有买入记录”会把手工
        成交算成平台仓。内存计数在全平/桥镜面归 0 时同步清零，天然支持
        「外部仓卖出后平台再买入」重新占位（再次成交时 apply_trade 累加）。

        作用域不变：ctx 是 ``portfolio.find_strategy(order.strategy_id)`` 返回的
        该组合策略自己的虚拟持仓，按组合策略各自计（与原来数 ctx.positions 一致）。
        """
        return sum(
            1 for p in (getattr(ctx, "positions", None) or {}).values()
            if p.quantity > 0 and getattr(p, "platform_quantity", 0) > 0)

    def _slot_limit(self, portfolio, ctx) -> Optional[int]:
        """取该组合策略自身的槽位上限；None = 参数缺失 → 跳过槽位闸（不限）。

        取值链路：strategies.max_positions（该策略行）→ assemble_portfolio →
        StrategyContext.max_positions → effective_max_positions(regime)。
        None 时绝不回落 5（5 只是成长组合自己的回测值，其他组合各不相同），
        只告警一次并放行（跳过槽位闸）——拒绝启动会让运行中的实盘会话停摆，
        而“不限”至少保持交易连续，缺口由 WARNING 暴露给人补配置。
        """
        pos_limit = ctx.effective_max_positions(
            getattr(portfolio, "current_regime", None))
        if pos_limit is None:
            key = (portfolio.portfolio_id, ctx.strategy_id)
            if key not in self._slot_limit_warned:
                self._slot_limit_warned.add(key)
                logger.warning(
                    "portfolio %s strategy %s: max_positions 缺失（strategies."
                    "max_positions 为空）→ 槽位闸跳过（不限持仓数），不回落默认 5；"
                    "请在组合策略配置补上该参数（session %s）",
                    portfolio.portfolio_id, ctx.strategy_id, self.session_id)
        return pos_limit

    # ------------------------------------------------------------------
    # 同一根 bar 只下一次单的**持久**幂等标记（2026-11-11）
    # ------------------------------------------------------------------
    def _bar_mark_seq(self, order: OrderEvent) -> int:
        """同一次决策内的大额拆单子单序号（0=普通单/拆单父单）。"""
        return int(getattr(order, "_split_seq", 0) or 0)

    def _bar_mark_key(self, order: OrderEvent, seq: Optional[int] = None):
        """(session, 组合, 策略, 股票, bar_time, 方向, seq)；bar_time 缺失 → None。"""
        if order.bar_time is None:
            return None
        return (
            self.session_id,
            order.portfolio_id,
            order.strategy_id,
            order.stock_code,
            order.bar_time,
            "buy" if order.trade_type == TradeType.BUY else "sell",
            self._bar_mark_seq(order) if seq is None else seq,
        )

    def _bar_already_processed(self, db: Session, order: OrderEvent) -> Optional[int]:
        """该 bar/方向是否已产生过下单动作（不看订单终态）。返回标记行 id 或 None。

        判据是「历史上提交过一次」，故原单 canceled/rejected 后重启重驱同一根 bar
        仍会被拦住——旧 D6 门只在 submitted/partial/filled 时生效，覆盖不到。
        """
        key = self._bar_mark_key(order)
        if key is None:
            return None
        row = db.query(LiveBarOrderMark.id).filter(
            LiveBarOrderMark.live_session_id == key[0],
            LiveBarOrderMark.portfolio_strategy_id == key[1],
            LiveBarOrderMark.strategy_id == key[2],
            LiveBarOrderMark.stock_code == key[3],
            LiveBarOrderMark.bar_time == key[4],
            LiveBarOrderMark.direction == key[5],
            LiveBarOrderMark.seq == key[6],
        ).first()
        return row[0] if row is not None else None

    def _mark_bar_processed(self, db: Session, order: OrderEvent,
                            seq: Optional[int] = None) -> bool:
        """写「该 bar 已处理」标记，唯一索引冲突即忽略（不抛、不重复）。

        返回 False 表示标记已存在（同一键被并发/重复提交）——调用方必须把该单
        视为重复并回滚，不能继续发桥。
        """
        key = self._bar_mark_key(order, seq)
        if key is None:
            return True
        stmt = sqlite_insert(LiveBarOrderMark).values(
            live_session_id=key[0], portfolio_strategy_id=key[1],
            strategy_id=key[2], stock_code=key[3], bar_time=key[4],
            direction=key[5], seq=key[6],
        ).on_conflict_do_nothing(index_elements=[
            "live_session_id", "portfolio_strategy_id", "strategy_id",
            "stock_code", "bar_time", "direction", "seq",
        ])
        result = db.execute(stmt)
        return bool(result.rowcount)

    def _clear_bar_mark(self, db: Session, key) -> None:
        """删除本次提交写入的「该 bar 已处理」标记（P0-1 异常回滚专用）。

        `_mark_bar_processed` 与订单行同事务落库；异常回滚后若不清掉标记，
        同一 bar 同向重试会在 `_mark_bar_processed` 撞唯一索引被静默回滚
        （= 当天该信号永久丢失）。key 为 None（bar_time 缺失 / 追价重下不写
        标记）时直接返回。清标记失败只告警，不阻断回滚主流程。
        """
        if key is None:
            return
        try:
            db.query(LiveBarOrderMark).filter(
                LiveBarOrderMark.live_session_id == key[0],
                LiveBarOrderMark.portfolio_strategy_id == key[1],
                LiveBarOrderMark.strategy_id == key[2],
                LiveBarOrderMark.stock_code == key[3],
                LiveBarOrderMark.bar_time == key[4],
                LiveBarOrderMark.direction == key[5],
                LiveBarOrderMark.seq == key[6],
            ).delete(synchronize_session=False)
            db.commit()
        except Exception:  # noqa: BLE001 — 清标记失败不改变已回滚的订单状态
            logger.warning(
                "clear bar mark failed: %s (session %s)", key, self.session_id)
            db.rollback()

    def _dispatcher_may_have_sent(
        self,
        prev_enqueued_oid: Optional[str],
        prev_unconfirmed_oid: Optional[str],
        prev_broker_oid: Optional[str],
    ) -> bool:
        """execute 抛非 Bridge* 异常时判定「委托是否可能已发出」（P0-1）。

        各 dispatcher 的 `place_order` 起始处都会重置 `last_enqueued_order_id` /
        `last_unconfirmed_order_id` / `last_broker_order_id` 三个哨兵，只有真正走到
        发单路径（入队 / 柜台受理未认领 / 拿到柜台委托号）才会写新值。因此
        「本次 execute 后任一哨兵为非 None 且与进入前不同」= 请求已发出。

        已发出时柜台可能已受理，**绝不能**把 DB 行回滚成 rejected——那会让一笔
        真实在途单脱离跟踪（无 ref、成交不回填、无法撤）。
        """
        for attr, prev in (
            ("last_enqueued_order_id", prev_enqueued_oid),
            ("last_unconfirmed_order_id", prev_unconfirmed_oid),
            ("last_broker_order_id", prev_broker_oid),
        ):
            cur = getattr(self._dispatcher, attr, None)
            if cur is not None and cur != prev:
                return True
        return False

    def _submit_one_order(
        self,
        db: Session,
        portfolio: Portfolio,
        order: OrderEvent,
        period: Optional[str],
        emit_signal: bool = True,
        post_close: bool = False,
        price_type: Optional[str] = None,
    ) -> None:
        """提交单笔订单走完整闸门链（_handle_bar 与撤单追价重下共用，批次1 3a）。

        闸门序（与原 _handle_bar 内联段逐字等价，continue→return）：
        1w/1mon signal SSE → 收盘后守卫 → 交易日历总闸 → B-1 未来函数 deferred
        （risk_timing=immediate 跳过）→ 资金/T+1 审批 → 跨重启去重（D6）→
        在途单门（F7）→ 落 submitted → 发桥（桥离线/拒单收口）。
        闸门拦截时静默返回（决策事件已记录）；异常向上抛由调用方回滚。

        price_type：仅盘前集合竞价路径传 ``"limit"``（按单覆盖桥的委托价类型，
        prType=11、审批价原样下发）；None = 通道按配置默认（盘中 opponent），
        其它调用点一律不传，行为不变。
        """
        ctx = portfolio.find_strategy(order.strategy_id)
        if ctx is None:
            return
        # 每单价型覆盖（P4）：deferred 放行按单下发 limit，未显式传参时取订单上
        # 的标记（None = 通道按配置默认，盘中单不受影响）。
        if price_type is None:
            price_type = getattr(order, "_price_type_override", None)
        # P1-5（2026-11-11）：分钟级信号单提交价让 1 跳——信号 bar 收盘价逆势时
        # 系统性欠成交（TDX 限价 / THS 键入价）。开关复用 requote_enabled（默认关
        # = 行为与旧版逐字一致），日线及以上路径与 deferred 放行价不受影响。
        self._apply_minute_price_bias(portfolio, order, ctx)
        # 1w/1mon 的 signal_cache 由注入路径提供、monitor 不重算，
        # 保留旧成单 SSE 避免前端丢信号；其余周期由 monitor 统一发。
        # 追价重下单非新信号（emit_signal=False），不重复推。
        # 同一 (bar_time, 组合, 策略, 票, 信号) 只推一轮：daily_closer 依次驱动
        # 1d/1w/1mon 会让同一批风控信号在 1w/1mon 各推一次（重复两轮）。
        if emit_signal and (self._signal_monitor is None or period in ("1w", "1mon")):
            sig_key = None
            if order.bar_time is not None:
                sig_key = (
                    order.bar_time, portfolio.portfolio_id, order.strategy_id,
                    order.stock_code,
                    order.signal_type.value if order.signal_type else None,
                    order.signal_name,
                )
            if self._first_emit_for_key(sig_key):
                self._emit("signal", {
                    "portfolio_id": portfolio.portfolio_id,
                    "strategy_id": order.strategy_id,
                    "stock_code": order.stock_code,
                    "signal_name": order.signal_name,
                    "signal_type": order.signal_type.value if order.signal_type else None,
                    "bar_time": order.bar_time.isoformat() if order.bar_time else None,
                })
        # 收盘后不下单守卫：bar 结束时间（bar_time）≥ 15:00（深交所收盘）则信号
        # 已推但跳过落单。根因：真机 2026-08-14 id40-41 的 bar_time=15:02:42（已过
        # 15:00），报不进交易所成待报单冻结持仓永不成交。用 bar_time（非墙钟：
        # 不依赖本机时钟/时区，测试与真实部署一致；主循环 60s 延迟不影响——延迟
        # 处理的是 15:00 前的合法 bar，其 bar_time<15:00 不被拦，正常下单）。
        # 不写 DB、不进 pending。
        bt = order.bar_time
        if bt is not None and (bt.hour, bt.minute) >= _MARKET_CLOSE_TIME:
            logger.info(
                "skip after-close order %s %s %s bar=%s (bar_time >= 15:00)",
                order.trade_type.value, order.stock_code, order.signal_name,
                bt,
            )
            self._rec_live(
                "after_close_block", "block", portfolio, order,
                message="bar_time %s >= 15:00 收盘后不下单" % bt,
            )
            return
        # P2（2026-11-09）：墙钟硬闸——分钟级单在 15:00 之后才被处理（主循环 60s
        # 延迟 / 队列积压 / 桥慢）一律拒绝。bar_time 守卫只能拦「bar 本身晚于 15:00」，
        # 拦不住 14:59 的 bar 拖到 15:01 才走到提交（此时报价/撮合已不可用，报单必成
        # 待报废单并冻结持仓）。日线级单不适用（post_close 恒 defer，由闸门另行处理）。
        if period not in self._DAILY_PERIODS and order.bar_time is not None:
            # 用注入时钟（ctx.clock，默认上海时区）。只在「墙钟与 bar 同一自然日」时
            # 生效：这才是「14:59 的 bar 拖到 15:01 才提交」的场景；跨日（历史 bar
            # 回放/单测固定日期）不适用，避免误挡。
            wall = self.ctx.clock()
            if wall.date() != order.bar_time.date():
                wall = None
            if wall is not None and (wall.hour, wall.minute) >= _MARKET_CLOSE_TIME:
                logger.info(
                    "skip after-close order %s %s %s wall=%s period=%s (wall >= 15:00)",
                    order.trade_type.value, order.stock_code, order.signal_name,
                    wall.strftime("%H:%M:%S"), period,
                )
                self._rec_live(
                    "after_close_block", "block", portfolio, order,
                    message="墙钟 %s >= 15:00，分钟级信号已过期不下单"
                            % wall.strftime("%H:%M:%S"),
                )
                return
        # 交易日历总闸：非交易日不下新单。用 bar_time（非墙钟，见上方收盘守卫同理）。
        # 桥日历时 fail-open（工作日放行），只在权威日历明确判定"非交易"时才拦——
        # 周末/节假日绝不误下，真实交易日不误挡。风控 update_peak 与 signal 事件仍
        # 照常执行（只拦落单）。
        # 日内周期(1m/5m...)额外卡交易时段 09:30–15:00；1d/1w/1mon 的 bar_time 按
        # 约定是当日 00:00（由 15:05「收盘落定」轮次 / B 的 14:52 提前轮次驱动），
        # 不套时段门，只判交易日，否则会把合法的日线单误杀。
        if bt is not None and not self._trading_allowed_for(bt, period):
            logger.info(
                "skip non-trading order %s %s %s bar=%s period=%s (calendar/closed)",
                order.trade_type.value, order.stock_code, order.signal_name,
                bt, period,
            )
            self._rec_live(
                "non_trading_block", "block", portfolio, order,
                message="非交易日/非交易时段 period=%s bar=%s" % (period, bt),
            )
            return
        # B-1 未来函数闸门（与回测同式判定 t.date() <= signal_bar_date）：
        # 日线级信号（signal_bar_date 非 None，signal bar 为当日 00:00，close
        # 未确认）当日不下单——实盘若当日即真实下单、回测却次日开盘成交，
        # 回测结果对实盘不可预测。订单进 deferred 队列，次一交易日首 bar 放行。
        # 持久化：落 status=deferred 行，行 id 挂单上；recover/stop 均以 DB 行
        # 为权威足迹（2026-11-09 起 recover 不再删行）。
        # 批次1 risk_timing=immediate（当日确认立即执行）跳过本闸门，
        # 当日直接提交；next_day（默认）维持次日执行。
        # post_close（15:05 收盘落定轮次）无条件 defer：收盘后报单报不进交易所，
        # 只会成为待报/废单，immediate 策略也不例外。
        if self._should_defer_order(portfolio, order, post_close=post_close):
            # A 源头幂等（长期防护）：同一 deferred 键只保留一行。2026-11-04 实证
            # 日线轮被重复驱动 → 同股同向 deferred 重复落库（重复止损卖单）。已存在
            # 同键行 → 跳过落库，打可读日志指出已存在行 id（不静默）；仅作用在
            # deferred 落库层，不影响已提交/已成交流程。
            dup = self._find_duplicate_deferred_row(db, order)
            if dup is not None:
                logger.warning(
                    "defer duplicate skipped (session %s): portfolio=%s strategy=%s "
                    "%s %s qty=%s signal_bar_date=%s; existing deferred row id=%s",
                    self.session_id, order.portfolio_id, order.strategy_id,
                    order.trade_type.value, order.stock_code, order.quantity,
                    order.signal_bar_date, dup.id,
                )
                # 内部幂等提示不推给工作台（用户可见事件只保留决策/信号本身）：
                # 同键 deferred 已由首轮 decision 事件承载，这里只留日志。
                # 该行若尚未在内存队列（如未经 recover 的场景）则接管进队，避免
                # 既有行既不执行也不清理；已在队则直接丢弃本次重复单。
                if not any(getattr(o, "_deferred_row_id", None) == dup.id
                           for o in self._signal_bar_deferred):
                    order._deferred_row_id = dup.id
                    self._signal_bar_deferred.append(order)
                return
            self._signal_bar_deferred.append(order)
            # P2 持久化：deferred 落库（status=deferred），重启 recover 重建
            # 内存队列——否则日线级止损/信号单随重启消失、永不执行
            # （2026-11-03 实证：止损 defer 57 次零执行）。行 id 挂单上，
            # 放行转 submitted 时删除旧行（防同一逻辑单双行）。
            try:
                lo = self._persist_order_submitted(db, order, status="deferred")
                db.commit()
                order._deferred_row_id = lo.id
            except Exception:  # noqa: BLE001
                db.rollback()
                logger.exception(
                    "deferred order persist failed (session %s)", self.session_id)
            self._rec_live(
                "signal_bar_date", "defer", portfolio, order,
                message="日线级信号次日执行：signal_bar_date=%s，本 bar %s 不下单，"
                        "延至次一交易日首 bar 提交" % (order.signal_bar_date, order.bar_time),
            )
            return
        # BUY 首次建仓：确保 Position 存在（同回测 BacktestEngine；submitted 不 apply）
        pos = ctx.positions.get(order.stock_code)
        if pos is None and order.trade_type == TradeType.BUY:
            pos = Position(order.stock_code)
            ctx.positions[order.stock_code] = pos
        # F5/BUY 量上限：先定最终下单量（账户资金审批 / 桥 T+1 可用）再落
        # submitted——DB 下单量与实发一致，回填不误判 partial。None=不通过不下单。
        # B-2 策略级敞口上限：与回测 _exposure_kwargs 完全同式透传
        # （strategy_position_value/strategy_capital_limit），多策略上限之和
        # 可超 100% 时实盘敞口不再系统性大于回测。
        # （提交价不在此刷新：分钟级信号价=最新完成 bar 收盘（C6 刻意语义），
        # 真正跨日过期的 deferred 放行单在 _release_signal_bar_deferred 刷新。）
        # BUY 审批资金对齐：min(虚拟现金 − 本组合在途 BUY 预留, 真实可用×0.98)。
        # 预留从 _pending_orders 派生（同 bar 先提交的单尚未回填、账本现金未扣；
        # 真实可用快照在 bar 内可能滞后），逐单复核因此不会重复使用同一笔现金。
        cash_cap = None
        if order.trade_type == TradeType.BUY:
            cash_cap = self._buy_available_cash(portfolio)
        capped = self._engine.cap_quantity(
            order, portfolio.account, pos,
            available_cash_cap=cash_cap,
            **self._exposure_kwargs(portfolio, ctx),
        )
        if capped is None:
            logger.info(
                "skip order %s %s %s: cap_quantity None（资金/持仓上限拦截，不下单）",
                order.trade_type.value, order.stock_code, order.signal_name,
            )
            # P1（2026-11-09）：审批拦截必须留决策事件——否则工作台只看到信号、
            # 看不到「为什么没执行」（资金不足 / T+1 可卖量不足 / 仓位上限）。
            self._rec_live(
                "approval", "block", portfolio, order,
                message=("资金/T+1/仓位审批未通过（cap_quantity=None），本单不下单；"
                         "目标 %s 股%s"
                         % (order.quantity,
                            ("，可用现金上限 %s" % cash_cap)
                            if cash_cap is not None else "")),
            )
            return
        order.quantity = capped
        # 成交量约束（实盘化，对齐回测 #2）：快照合成 bar 量 × volume_limit_ratio。
        # BUY 薄量拒单标 _volume_capped_out → 不落库不进在途（量足后的信号重试）。
        self._apply_volume_cap(order, portfolio)
        if getattr(order, "_volume_capped_out", False):
            return
        # 跨重启去重门（D6）：同 (组合/策略/股票/bar_time/方向) 已有未完结单则跳过。
        # 根因：14:30 后页面停止/再启动 = Core 重启 = 新 LiveEngine = 实例内存丢失，
        # 1d/1w/1mon 会被重新驱动到同一 bar_time，且 order_id=live_order.id（自增 PK）
        # 每次重启都新号，桥侧 _placed 幂等无效 → 重复下单。
        # 按 (策略+股票+bar_time+方向) 去重：分钟策略不同 bar（10:00 vs 10:05）不被误杀、
        # 同组合多策略同 bar 同股各自开仓不被误拦，仅「同策略重驱到同 bar 同方向」才拦。
        # BUY/SELL 对称去重：重复 SELL 危害不亚于重复 BUY（已卖完再卖 → 超卖/桥拒单噪音）。
        # 分钟策略不同 bar 的合法平仓不受影响（bar_time 不同即不拦）。
        if order.bar_time is not None:
            op = "buy" if order.trade_type == TradeType.BUY else "sell"
            dup = db.query(LiveOrder).filter(
                LiveOrder.live_session_id == self.session_id,
                LiveOrder.portfolio_strategy_id == order.portfolio_id,
                LiveOrder.strategy_id == order.strategy_id,
                LiveOrder.stock_code == order.stock_code,
                LiveOrder.bar_time == order.bar_time,
                LiveOrder.trade_type == op,
                LiveOrder.status.in_(["submitted", "partial", "filled"]),
            ).first()
            if dup is not None:
                logger.info(
                    "skip dup %s %s %s bar=%s already placed (order %s)",
                    op.upper(), order.stock_code, order.signal_name,
                    order.bar_time, dup.id,
                )
                self._rec_live(
                    "dup_skip", "block", portfolio, order,
                    message="同 bar 同向单已存在 order=%s" % dup.id,
                )
                return
        # 持久幂等门（2026-11-11）：同 (session/组合/策略/股票/bar_time/方向, 子单
        # 序号) 历史上提交过一次即视为该 bar 已处理——**不看订单当前终态**。上面的
        # D6 门只覆盖 submitted/partial/filled，原单 canceled/rejected 后重启重驱
        # 同一根 bar（1d/1w/1mon 典型）会再次下单，只能靠持仓/资金/在途闸兜底。
        # 豁免：追价重下（同一张单的加价重报，不是新决策）；盘前 deferred 放行会把
        # bar_time 改写成当日（新键，自然通过）；拆单子单按 _split_seq 带序号区分。
        if not getattr(order, "_requote", False):
            prev_mark = self._bar_already_processed(db, order)
            if prev_mark is not None:
                logger.info(
                    "skip bar-processed %s %s %s bar=%s (mark %s)",
                    order.trade_type.value, order.stock_code, order.signal_name,
                    order.bar_time, prev_mark,
                )
                self._rec_live(
                    "bar_processed", "block", portfolio, order,
                    message="跳过：该 bar 已处理（mark=%s，同 bar 同向历史已下过单，"
                            "不看原单终态）" % prev_mark,
                )
                return
        # 在途单门（F7）：同 (组合,策略,股票) 已有同向未确认单（submitted/partial）
        # → 压掉新同向单。根因：下单→/deals 成交回填之间存在在途窗口，期间虚拟
        # 持仓未更新，连续 bar 的 OPEN/CLOSE 信号看持仓是旧的 → 同股同向重复
        # 买单/卖单（真机 2026-08-13：159888/159929/159936 两根连续 bar 两次 OPEN）。
        # 按「同向」拦：在途 BUY 不拦 SELL、在途 SELL 不拦 BUY——风控止损/平仓
        # 卖出不被未确认的买入挡住。bar_time 不是放行理由（不同 bar 只是两次
        # 信号时点，上一单未确认就再下同向单仍是重复）。回填确认（filled）后由
        # portfolio.py 持仓守卫接管；rejected 释放门（被拒不会成交，必须允许重试）。
        # 终态（filled/canceled/rejected）都不在 submitted/partial 内，正确释放门；
        # 仅在途单占门。比重复买入更安全（prType=14 秒成秒回填，正常路径不受影响）。
        # 追价重下单在原单收敛 canceled 之后构造（不在途），天然过门。
        # 在途闸口径统一（2026-11-10）：已打 _UNTRACEABLE_MARKER 的单（桥通道 180s
        # 仍查不到 order_ref）**不再占用同股同向门**——否则一笔不可追溯的止损卖单
        # 会把后续止损/平仓压最长 10 分钟（桥通道账务收口窗口 600s）。
        # 它们仍是 submitted（不判 rejected、迟到成交仍回填纠正），只是不再挡新单：
        # 180s 内 /orders 与 /deals 双双查不到，几乎只能是「没报到柜台」，而真在途
        # 的单会在 /orders 里被 order_ref 追平（那时不打标记、继续占门）。
        op = "buy" if order.trade_type == TradeType.BUY else "sell"
        inflight = db.query(LiveOrder).filter(
            LiveOrder.live_session_id == self.session_id,
            LiveOrder.portfolio_strategy_id == order.portfolio_id,
            LiveOrder.strategy_id == order.strategy_id,
            LiveOrder.stock_code == order.stock_code,
            LiveOrder.trade_type == op,
            LiveOrder.status.in_(["submitted", "partial"]),
            or_(
                LiveOrder.error_message.is_(None),
                ~LiveOrder.error_message.like(_UNTRACEABLE_MARKER + "%"),
            ),
        ).first()
        if inflight is not None:
            logger.info(
                "skip inflight %s %s %s bar=%s (order %s still %s)",
                op.upper(), order.stock_code, order.signal_name,
                order.bar_time, inflight.id, inflight.status,
            )
            self._rec_live(
                "inflight_skip", "block", portfolio, order,
                message="同向在途单未确认 order=%s status=%s" % (inflight.id, inflight.status),
            )
            return
        # 人工核对闸（2026-11-11）：桥通道超过 grace 仍无法追溯的单不再判 rejected，
        # 而是打 bridge_order_manual_review 标记留在途（柜台可能仍挂着旧单）。这里挡住
        # 同 (组合策略, 策略, 代码, 方向) 的自动重报，直到人工核对或柜台回读终态——
        # 防止「旧单还在柜台 + 平台又报一张」的双份成交。只拦同向：反向单是独立决策，
        # 仍走各自的资金/持仓/槽位闸。
        review = db.query(LiveOrder).filter(
            LiveOrder.live_session_id == self.session_id,
            LiveOrder.portfolio_strategy_id == order.portfolio_id,
            LiveOrder.strategy_id == order.strategy_id,
            LiveOrder.stock_code == order.stock_code,
            LiveOrder.trade_type == op,
            LiveOrder.status.in_(["submitted", "partial"]),
            LiveOrder.error_message.startswith(_MANUAL_REVIEW_MARKER,
                                              autoescape=True),
        ).first()
        if review is not None:
            logger.error(
                "block %s %s %s: order %s awaits manual review "
                "(untraceable past grace, avoid duplicate fill)",
                op.upper(), order.stock_code, order.signal_name, review.id,
            )
            self._rec_live(
                "manual_review", "block", portfolio, order,
                message="同向委托 %s 超过 grace 仍无法追溯，已保留在途等待人工核对，"
                        "禁止自动重报" % review.id,
            )
            return
        # 批次1 外部信号改造：BUY 槽位/排重闸（SELL 不受限——风控卖出必须放行）。
        # ① 槽位闸：已持仓数 + 本策略在途 BUY 单数 >= max_positions → 拦截
        # （portfolio.on_bar 的 OPEN 闸用内存 _pending_orders 口径，此处用 DB
        # 口径兜底——跨池回填/重启恢复的在途单不漏数）。已持仓股票的 BUY
        # （ADD 补仓）不占新槽位，槽位闸不适用；拆单子单豁免（同一笔逻辑单的
        # 等量切片，槽位/排重按父单口径判定）。
        # ② 排重闸（T+1）：该策略该股票当日已有 BUY 成交（live_trades）→ 重复
        # 信号拦截——通达信条件预警是电平信号会反复触发，当日二次开仓不交易。
        if (order.trade_type == TradeType.BUY
                and not getattr(order, "_split_child", False)
                and not self._split_children.get(
                    (order.strategy_id, order.stock_code))):
            # 拆单队列在途（同策略同股）→ 本单是大额拆单的后续信号，
            # 交由 _maybe_split_order 排队（既有拆单语义，闸门不拦）。
            held_pos = ctx.positions.get(order.stock_code)
            if held_pos is None or held_pos.quantity <= 0:
                pos_limit = self._slot_limit(portfolio, ctx)
                # 槽位只数「平台自己买入成交过」的持仓（方案 A）：外部/手工导入仓
                # （如 510050）不占名额；None=该策略未配置 → 跳过槽位闸（见 _slot_limit）
                held = self._platform_held_count(ctx)
                inflight_buys = db.query(LiveOrder).filter(
                    LiveOrder.live_session_id == self.session_id,
                    LiveOrder.portfolio_strategy_id == order.portfolio_id,
                    LiveOrder.strategy_id == order.strategy_id,
                    LiveOrder.trade_type == "buy",
                    LiveOrder.status.in_(["submitted", "partial"]),
                ).count()
                if pos_limit is not None and held + inflight_buys >= pos_limit:
                    logger.info(
                        "skip buy %s %s: positions full (%d platform held + %d inflight >= %d)",
                        order.stock_code, order.signal_name,
                        held, inflight_buys, pos_limit,
                    )
                    self._rec_live(
                        "position_full", "block", portfolio, order,
                        message="槽位已满：平台买入持仓 %d + 在途 BUY %d >= max_positions %d，"
                                "跳过 BUY" % (held, inflight_buys, pos_limit),
                    )
                    return
            # T+1 排重按订单 bar_time 的自然日（bar 驱动口径，与收盘/日历闸一致；
            # bar_time 缺省回退当前上海时间）。
            ref_day = (order.bar_time or now_shanghai()).date()
            day_start = datetime(ref_day.year, ref_day.month, ref_day.day)
            bought_today = db.query(LiveTrade).filter(
                LiveTrade.live_session_id == self.session_id,
                LiveTrade.portfolio_strategy_id == order.portfolio_id,
                LiveTrade.strategy_id == order.strategy_id,
                LiveTrade.stock_code == order.stock_code,
                LiveTrade.trade_type == "buy",
                LiveTrade.trade_time >= day_start,
            ).first()
            if bought_today is not None:
                logger.info(
                    "skip duplicate-signal buy %s %s: already bought today "
                    "(trade %s order %s)",
                    order.stock_code, order.signal_name,
                    bought_today.id, bought_today.live_order_id,
                )
                self._rec_live(
                    "duplicate_signal", "block", portfolio, order,
                    message="同股当日已有 BUY 成交（trade %s，T+1 排重），跳过重复信号"
                            % bought_today.id,
                )
                return
        # 批次2：TDX 大额拆单——资格命中则不提交原单，拆为等量子单队列：
        # 仅立即提交子单 1，其余由 _tick_split_children 在前子单成交回填后
        # 逐笔续发（F7 同向在途闸：前一子单未离场前续发必被拦；真实柜台按笔
        # 独立冻结资金，顺序提交不等成交会超预算——成交结算后子单审批现金才对）。
        # 资格判定在原单落库前——原单不落库，子单 1 带同 bar_time 落库，
        # 重启重驱同一信号由 D6 去重挡住，不会重复下单。
        if self._maybe_split_order(db, portfolio, order, ctx, period):
            return
        # ① 先写 submitted + commit（I4 命门窗口闭合）；计入在途集合（G7 计数）
        # 记账锁的范围必须把写库语句一起包住：若本线程先 flush（持有 sqlite 写锁）
        # 再去等 _book_lock，而 deals 池正持 _book_lock 做 commit，就会互相等待
        # 到 sqlite busy timeout（~5s）并丢掉一轮成交回填。故先取锁再落库。
        # 在途表增删与 deals 池的 sync_pending_orders（按 DB 查询整表替换）串行化：
        # dict 写入与 commit 必须同锁——否则 DB 查询先于 commit 的整表替换会把本单
        # 丢掉，该 bar 的 inflight_opens 占位失效（max_positions 漏计 → 超开一仓），
        # 且 _tick_split_children 遍历期间并发增删会抛
        # "dictionary changed size during iteration"（整轮回填作废）。
        # _book_lock 为 RLock，_requote_* 在锁内二次进入安全；锁序仍为
        # _book_lock → get_tdx_lock，无反向获取。
        with self._book_lock:
            # 放行的 deferred 单：删除 B-1 时落库的 deferred 旧行（防同一逻辑单双行）
            # 并在同事务内写 submitted 新行——外部任何时刻都看得到该单的足迹
            # （deferred 或 submitted 必居其一）。行 id 随单挂在 _deferred_row_id，
            # 重启 recover 重建的单同样带此标记（2026-11-09 起）。
            _dr = getattr(order, "_deferred_row_id", None)
            # P0-1：放行单标记（异常回滚时决定「回 deferred 重排」还是「落 rejected」）
            had_deferred_row = bool(_dr)
            # P0-1：本次写入的 bar 幂等标记键（回滚时必须清掉，否则同 bar 重试被判重）
            bar_mark_key = None
            if _dr:
                db.query(LiveOrder).filter(LiveOrder.id == _dr).delete()
                order._deferred_row_id = None
            live_order = self._persist_order_submitted(db, order)
            # 持久幂等标记与订单行**同事务**落库：唯一索引兜底并发（应用层先查后写
            # 在多线程下会漏）。冲突 = 同一根 bar 已被处理 → 回滚本单，绝不发桥。
            # 追价重下不写标记（同一张单的加价重报，原单标记已在）。
            if not getattr(order, "_requote", False):
                bar_mark_key = self._bar_mark_key(order)
                bar_mark_ok = self._mark_bar_processed(db, order)
                if not bar_mark_ok:
                    lo_id = live_order.id
                    db.rollback()
                    logger.warning(
                        "bar-processed conflict: %s %s bar=%s → 回滚本单（session %s）",
                        order.trade_type.value, order.stock_code,
                        order.bar_time, self.session_id,
                    )
                    self._rec_live(
                        "bar_processed", "block", portfolio, order,
                        message="跳过：该 bar 已处理（并发重复提交，已回滚本单 order=%s）"
                                % lo_id,
                    )
                    return
            live_order._submit_at = now_shanghai()
            self._pending_orders[live_order.id] = live_order
            # B5：订单状态推送（submitted——真实成交回报由 _poll_deals 回填后推 filled）
            self._emit("order", {
                "portfolio_id": portfolio.portfolio_id,
                "order_id": live_order.id,
                "strategy_id": live_order.strategy_id,
                "stock_code": live_order.stock_code,
                "trade_type": (live_order.trade_type or "").upper(),
                "status": "submitted",
                "quantity": live_order.quantity,
                "price": float(live_order.price) if live_order.price is not None else None,
                "bar_time": live_order.bar_time.isoformat() if live_order.bar_time else None,
            })
            db.commit()
        # THS file_builtin 入队标记：place_order 秒回 None 但指令已写入
        # 交接文件时非 None（=本单确定性 order_id）。其它通道恒 None。
        enqueued_oid: Optional[str] = None
        # 入队标记哨兵：本次 execute 若在前置审批就失败，place_order 根本不会被调用，
        # dispatcher 的 last_enqueued_order_id 会残留「上一单」的值。比较哨兵后
        # 只有本次新写入的标记才算入队，避免把 approval failed 误判成已受理。
        prev_enqueued_oid = getattr(self._dispatcher, "last_enqueued_order_id", None)
        # 「已受理未认领」哨兵（TDX）：place_order 柜台受理成功但回查窗口内委托表
        # 未见该单 → 返回 None 并留本单 order_id 标记。与入队哨兵同理，只有本次
        # 新写入的标记才有效，避免残留值把 approval failed 误判成已受理。
        prev_unconfirmed_oid = getattr(
            self._dispatcher, "last_unconfirmed_order_id", None)
        # P0-1：柜台委托号哨兵（桥受理应答带 cl_ord_id）——与上面两个哨兵同理，
        # 用于判定「请求是否已真正发出」，决定异常时能否安全回滚。
        prev_broker_oid = getattr(self._dispatcher, "last_broker_order_id", None)
        try:
            # 涨跌停前置校验（price_limit_check_enabled 配置门控，通道无关，
            # 口径与回测同源）：approve（cap_quantity）之后、place_order 之前。
            # 返回拒绝原因 → raise BridgeOrderRejected 沿用既有拒单路径
            # （rejected 落库 + 决策事件 + SSE 回显原因）。配置关闭 → 完全跳过；
            # 通道未实现该校验（测试替身/未来新通道）→ 视为 fail-open 放行。
            check_price_limit = getattr(
                self._dispatcher, "check_price_limit", None)
            if portfolio.price_limit_check_enabled and check_price_limit is not None:
                reason = check_price_limit(
                    order.stock_code,
                    "buy" if order.trade_type == TradeType.BUY else "sell",
                    order.price,
                )
                if reason is not None:
                    raise BridgeOrderRejected(reason)
            # ② 再发 passorder（apply=False：submitted 阶段不更新账户持仓）
            # B-2：execute 内部二次 cap_quantity/approve 同样透传敞口上限
            # （跳空/价格变动后的重批与首审同口径）。
            # Fix4：buy_fee_cfg 透传组合费用口径——execute 发单前做 BUY
            # 费用感知前置钳制（approve 只卡价×量，尾款不足佣金会透支
            # 虚拟现金；桥通道无法发单后改量重发）。
            trade = self._engine.execute(
                order, portfolio.account, pos, apply=False,
                buy_fee_cfg=self._buy_fee_cfg(portfolio),
                price_type=price_type,
                **self._exposure_kwargs(portfolio, ctx),
            )
        except BridgeUnavailableError:
            # 桥离线：标 rejected + 置离线暂停（_on_bar 上层心跳循环据此暂停下单）
            live_order.status = "rejected"
            live_order.error_message = "bridge unavailable"
            # P2-6：下单时才发觉的断线也要记缺口起点（心跳在本轮开头可能还是通的）
            self._mark_bridge_offline(now_shanghai())
            self._rec_live(
                "bridge_unavailable", "reject", portfolio, order,
                message="桥离线（dispatcher 不可达）",
            )
            with self._book_lock:
                self._pending_orders.pop(live_order.id, None)
                self._emit("order", {
                    "portfolio_id": portfolio.portfolio_id,
                    "order_id": live_order.id,
                    "status": "rejected",
                    "stock_code": live_order.stock_code,
                    "trade_type": (live_order.trade_type or "").upper(),
                    "quantity": live_order.quantity,
                    "bar_time": live_order.bar_time.isoformat() if live_order.bar_time else None,
                    "error_message": live_order.error_message,
                })
                db.commit()
            return
        except BridgeOrderRejected as e:
            # P1-3（2026-11-11）：价格闸拦截 / 桥明确拒单不再一次性作废——按回测
            # 口径回 deferred（次一交易日 due 窗口或同一交易日 09:31 首根 bar 再试），
            # 受 _DEFERRED_RETRY_MAX 上限约束；只有参数错误/资金不足/通道不支持这类
            # 真错误（_is_retryable_reject=False）或次数用尽才落终态 rejected。
            # 回显桥侧真实原因（此前被吞成笼统 "approval failed"，真机查不了原因）。
            if _is_retryable_reject(e) and self._requeue_rejected_order(
                    db, portfolio, order, live_order, str(e)):
                return
            live_order.status = "rejected"
            live_order.error_message = str(e)
            # 终态拒单不得留在 deferred 队列里（否则同一逻辑单会被反复重试）
            if any(id(o) == id(order) for o in self._signal_bar_deferred):
                with self._book_lock:
                    self._signal_bar_deferred[:] = [
                        o for o in self._signal_bar_deferred if id(o) != id(order)]
            self._rec_live(
                "bridge_rejected", "reject", portfolio, order,
                message="桥业务拒单: %s" % str(e),
            )
            with self._book_lock:
                self._pending_orders.pop(live_order.id, None)
                self._emit("order", {
                    "portfolio_id": portfolio.portfolio_id,
                    "order_id": live_order.id,
                    "status": "rejected",
                    "stock_code": live_order.stock_code,
                    "trade_type": (live_order.trade_type or "").upper(),
                    "quantity": live_order.quantity,
                    "bar_time": live_order.bar_time.isoformat() if live_order.bar_time else None,
                    "error_message": live_order.error_message,
                })
                db.commit()
            return
        except Exception as e:  # noqa: BLE001 — P0-1：兜住 TDXLockTimeout 等非 Bridge* 异常
            # P0-1（2026-11-12）：旧实现只捕 BridgeUnavailableError / BridgeOrderRejected，
            # TDXLockTimeout（TQ 锁超时）等其它异常直接外抛 → DB 留 submitted 幻影行、
            # `_pending_orders` 留幻影在途（`_dedupe_inflight_sells` 把同码同向重试判死）、
            # 本 bar 幂等标记已占键（重试被唯一索引静默拦下）→ 当天该信号永久丢失。
            # 语义：**只有「dispatcher 明确还没返回 order_id」才允许回滚**。
            detail = "%s: %s" % (type(e).__name__, e)
            if self._dispatcher_may_have_sent(
                    prev_enqueued_oid, prev_unconfirmed_oid, prev_broker_oid):
                # 请求已发出（柜台可能已受理）：保留 submitted，绝不回滚成 rejected
                # ——否则真实在途单脱离跟踪（无 ref、成交不回填、无法撤）。
                live_order.error_message = (
                    "submit_error_after_send: %s（请求已发出，柜台状态待核对，"
                    "保留 submitted 等回填/超时收口）" % detail)[:500]
                self._rec_live(
                    "submit_error", "keep", portfolio, order,
                    message=("发单后异常（%s）：请求已发出，保留 submitted 待核对，"
                             "不回滚、不重报" % detail),
                )
                with self._book_lock:
                    db.commit()
                detach_loaded(db, live_order)
                raise
            # 明确未发出：同事务回滚幻影（订单行 / 在途集合 / bar 幂等标记）
            self._rec_live(
                "submit_error", "rollback", portfolio, order,
                message=("发单异常（%s）且请求未发出：回滚本单（不留在途、不占 bar 标记）"
                         % detail),
            )
            if had_deferred_row and self._requeue_rejected_order(
                    db, portfolio, order, live_order,
                    "submit_error: %s" % detail):
                # 放行单：回 deferred 可重排（受既有次数上限约束），并清掉本次写入的
                # bar 标记，避免重试被 `_mark_bar_processed` 唯一索引静默拦下。
                self._clear_bar_mark(db, bar_mark_key)
                return
            live_order.status = "rejected"
            live_order.error_message = ("submit_error: %s" % detail)[:500]
            with self._book_lock:
                # 摘掉幻影在途；`_reserved_buy_cash` 按在途单实时汇总，pop 即释放预留
                self._pending_orders.pop(live_order.id, None)
                self._emit("order", {
                    "portfolio_id": portfolio.portfolio_id,
                    "order_id": live_order.id,
                    "status": "rejected",
                    "stock_code": live_order.stock_code,
                    "trade_type": (live_order.trade_type or "").upper(),
                    "quantity": live_order.quantity,
                    "bar_time": live_order.bar_time.isoformat() if live_order.bar_time else None,
                    "error_message": live_order.error_message,
                })
                db.commit()
            detach_loaded(db, live_order)
            self._clear_bar_mark(db, bar_mark_key)
            raise
        if trade is None:
            # TDX：柜台已受理（ErrorId=0）但委托表暂未见该单——**保持 submitted**，
            # 绝不能判 rejected（真实在途单会脱离跟踪：无 ref、成交不回填、无法撤，
            # 虚拟持仓/现金与柜台脱节）。后续 poll_deals 每轮仍做 legacy 模糊认领，
            # 最坏 180s expire 收口；SELL 先扣本 bar 可用量防同 bar 二次卖出。
            unconfirmed_oid = getattr(
                self._dispatcher, "last_unconfirmed_order_id", None)
            if (unconfirmed_oid and unconfirmed_oid != prev_unconfirmed_oid
                    and unconfirmed_oid == self._dispatcher.order_id(order)):
                live_order.bridge_order_id = self._dispatcher.order_id(order)
                # 受理应答已带柜台委托号（掘金 cl_ord_id）时同样受理即落库：桥的
                # /orders 回读常常当场查不到该单（当日委托查询不含废单、桥重启丢
                # 内存台账），不能因此丢掉唯一的精确认领键。DB 是记帐真相。
                broker_oid = getattr(self._dispatcher, "last_broker_order_id", None)
                if broker_oid and live_order.order_ref is None:
                    live_order.order_ref = str(broker_oid)
                # 原因优先取 dispatcher 的可读线索（HTTP 桥：超时/无委托标识/回查
                # 未确认；TDX：委托表暂未见），无则回退通道无关文案。
                live_order.error_message = (
                    getattr(self._dispatcher, "last_unconfirmed_message", None)
                    or "已受理未认领：委托表暂未见，等待回填/超时收口"
                )
                # 不再重复推 order/submitted：落库前已推过一次（见上方 _persist_order_submitted
                # 之后的 _emit），此处只补一条 order_unconfirmed 决策事件说明「已受理未认领」，
                # 前端不会看到两条一模一样的「提交」事件。error_message 已写进
                # live_order（随下次 order 事件/详情接口带出）。
                self._rec_live(
                    "order_unconfirmed", "warn", portfolio, order,
                    message="柜台已受理但暂未认领委托号，保持 submitted 等待回填",
                )
                if order.trade_type == TradeType.SELL:
                    self._t1_checker.consume_available(
                        order.stock_code, order.quantity)
                db.commit()
                # 在途集合只存已加载的 detached 对象（commit 已把它置为 expired）
                detach_loaded(db, live_order)
                logger.warning(
                    "order accepted but unclaimed: id=%s %s %s qty=%s (session %s)",
                    live_order.id, order.trade_type.value, order.stock_code,
                    live_order.quantity, self.session_id,
                )
                return
            # THS file_builtin：place_order 已把指令写入交接文件并秒回 None
            # → 本单已受理入队，保持 submitted 等内置执行器执行 + UI 回填
            # 链路确认成交（执行失败单由执行器落 results rejected 回执，
            # 残留 submitted 单由 180s expire 兜底标 rejected）。
            enqueued_oid = getattr(self._dispatcher, "last_enqueued_order_id", None)
            if (enqueued_oid and enqueued_oid != prev_enqueued_oid
                    and enqueued_oid == self._dispatcher.order_id(order)):
                logger.info(
                    "order enqueued to ths handoff: id=%s %s %s qty=%s (session %s)",
                    live_order.id, order.trade_type.value, order.stock_code,
                    live_order.quantity, self.session_id,
                )
            else:
                # 兜底：execute 内部审批不过（前置 cap 与 execute 间账户变化）
                # 或桥返回 {ok:false} 无 error（非 JSON 故障路径，#24）→ rejected
                live_order.status = "rejected"
                live_order.error_message = "approval failed"
                self._rec_live(
                    "approval_failed", "reject", portfolio, order,
                    message="发单兜底失败（审批不过/桥返回 ok:false 无原因）",
                )
                with self._book_lock:
                    self._pending_orders.pop(live_order.id, None)
                    self._emit("order", {
                        "portfolio_id": portfolio.portfolio_id,
                        "order_id": live_order.id,
                        "status": "rejected",
                        "stock_code": live_order.stock_code,
                        "trade_type": (live_order.trade_type or "").upper(),
                        "quantity": live_order.quantity,
                        "bar_time": live_order.bar_time.isoformat() if live_order.bar_time else None,
                        "error_message": live_order.error_message,
                    })
                    db.commit()
                return
        # ③ 桥受理成功：回写幂等 order_id，尝试同步定位 OrderRef（失败下轮回填再找）
        live_order.bridge_order_id = self._dispatcher.order_id(order)
        # 受理即落柜台委托号（掘金 cl_ord_id）：DB 是记帐真相——桥进程重启会丢内存
        # 台账（废单/已成交行当日委托查询也不回显），只有 DB 里的 order_ref 重启后仍
        # 能精确认领；后续 /orders、/deals 均按该 ref 匹配，无需再靠模糊匹配。
        # 注意：bridge_order_id 保持确定性 oid（其前 20 位是写进柜台的 remark 认领键），
        # 不能被 cl_ord_id 覆盖，两者语义不同。
        broker_oid = getattr(self._dispatcher, "last_broker_order_id", None)
        if broker_oid and live_order.order_ref is None:
            live_order.order_ref = str(broker_oid)
        # THS file_builtin：指令刚入队、执行器尚未真正下单，立即回查只会
        # 碰到 120s 认领窗内的同形状旧单（误绑 → 成交回填张冠李戴）。
        # ref 留 None，执行器下单后由 /deals 回填周期再定位（彼时本单是
        # 最新同形状行，与 direct 模式同一口径）。
        if enqueued_oid != live_order.bridge_order_id:
            # 批次2：提交时认领必须携带 DB 已占 refs——TDX/THS legacy 模糊匹配按
            # 代码+方向+数量认领，同票同向等量的后续单（拆单等量子单/同量重下）
            # 若 claimed 为空会把在提交窗口内可见的前单委托行绑到自己头上 →
            # /deals 按 ref 双份回填 → 持仓翻倍。带全量已占 ref 后前单被排除，
            # 只能绑自己的委托行（找不到则留 None 下轮回填再找，行为不变）。
            claimed_refs_now = {
                r[0] for r in db.query(LiveOrder.order_ref).filter(
                    LiveOrder.live_session_id == self.session_id,
                    LiveOrder.order_ref.isnot(None),
                ).all()
            }
            self._try_match_order_ref(live_order, claimed_refs=claimed_refs_now)
        # F6：SELL 已发单成功 → 从 bar 可用量扣减（同 bar 后续 SELL 见递减后的值，
        # 避免 A 卖 600+B 卖 400 超券商 available；扣过量钳到 0，券商端仍兜底）。
        if order.trade_type == TradeType.SELL:
            self._t1_checker.consume_available(order.stock_code, order.quantity)
        db.commit()
        # 在途集合只存已加载的 detached 对象（commit 已把它置为 expired；session
        # 在本批结束即 close，不摘出会变成 detached+expired → 下一根 bar 读
        # 在途单属性抛 DetachedInstanceError，打断整批提交）
        detach_loaded(db, live_order)
        logger.info(
            "order accepted: id=%s %s %s %s qty=%s price=%s bar=%s (session %s)",
            live_order.id, order.trade_type.value, order.stock_code,
            order.signal_name, live_order.quantity, live_order.price,
            order.bar_time, self.session_id,
        )

    # ---------------- 批次2：TDX 大额拆单（成交驱动子单队列）----------------

    def _maybe_split_order(self, db: Session, portfolio: Portfolio,
                           order: OrderEvent, ctx, period: Optional[str]) -> bool:
        """TDX 大额拆单资格判定 + 子单队列建立。True=已拆单（调用方不再提交原单）。

        资格：ctx.split_order_enabled 且通道 tdx_tq 且 price×quantity ≥
        split_order_threshold（阈值 ≤0 视为未配置不拆；金额=阈值 → n=1 不拆）。
        命中则按 n=ceil(金额/阈值) 拆等量子单（vol//n，余数依次补给前 vol%n 笔，
        Σ=原量，每子单金额 ≤ 阈值+一手内偏差），子单 1 立即走完整提交闸门链
        （独立审批 cap_quantity/exposure），其余入 _split_children 队列等续发。

        同 (策略,股票) 已有队列且队首子单同向 → 新单子单 append 排队（F7 保证
        同向在途唯一，撞键仅发生在「队首已成交待续发」窗口）。队首子单**反方向**
        → 不拆不排队，单笔直提（return False 走正常提交链）：反方向大单——尤其
        风控止损/清仓 SELL——排队等待 BUY 队列会拖垮退出时效，且 SELL 不冻结
        现金无超预算问题。子单带 _split_child
        标记不再二次拆分。子单 bar_time 毫秒级递增：D6 跨重启去重键互异，
        收盘守卫/日历/deferred 按 date/hour/minute 判定不受毫秒影响。
        """
        if getattr(order, "_split_child", False):
            return False
        if not getattr(ctx, "split_order_enabled", False):
            return False
        if getattr(self._dispatcher, "channel", None) != "tdx_tq":
            return False
        threshold = getattr(ctx, "split_order_threshold", None)
        if (order.price is None or not order.quantity or threshold is None
                or threshold <= 0):
            return False
        amount = order.price * order.quantity
        if amount < threshold:
            return False
        n = int((amount / threshold).to_integral_value(rounding=ROUND_CEILING))
        n = min(n, order.quantity)  # 防子单 0 股（阈值极小/股数极少时）
        if n <= 1:
            return False
        # 子单量必须整手：A 股（含可转债）买卖申报为 100 股（转债 10 张）整数倍
        # （余额不足一手一次性卖出除外）；科创板 688/689、北交所 4/8/92 允许
        # 1 股递增，不受整手约束。原实现直接 vol//n 均分——1000 股拆 3 笔得
        # 334/333/333，柜台按「非 100 整数倍」直接拒单，整组子单失败。
        # 故可整手拆分时按整手均分（余数按整手补给前几笔），否则回退按股均分。
        lot = 10 if order.stock_code[:2] in ("11", "12") else 100
        use_lot = (
            order.quantity % lot == 0
            and not order.stock_code.startswith(("688", "689", "4", "8", "92"))
        )
        if use_lot:
            total_lots = order.quantity // lot
            n = min(n, total_lots)
            if n <= 1:
                return False
            base_lots = total_lots // n
            rem_lots = total_lots - base_lots * n
            quantities = [
                (base_lots + (1 if i < rem_lots else 0)) * lot for i in range(n)
            ]
        else:
            base = order.quantity // n
            rem = order.quantity - base * n
            quantities = [base + (1 if i < rem else 0) for i in range(n)]
        children: List[OrderEvent] = []
        for i, qty in enumerate(quantities):
            child = replace(order, quantity=qty)
            child._split_child = True
            # 子单序号：持久幂等标记按 seq 区分，同一决策的多笔子单各成一键，
            # 不会第二笔被自己的第一笔挡住（父单固定 seq=0）。
            child._split_seq = i + 1
            if child.bar_time is not None:
                child.bar_time = child.bar_time + timedelta(milliseconds=i)
            children.append(child)
        split_msg = (
            "大额拆单：金额 %s ≥ 阈值 %s，拆为 %d 笔子单（%s 股），成交驱动依次提交"
            % (amount, threshold, n, "+".join(str(c.quantity) for c in children))
        )
        with self._book_lock:
            key = (order.strategy_id, order.stock_code)
            entry = self._split_children.get(key)
            if entry is not None:
                inflight = entry.get("inflight")
                if (inflight is not None
                        and getattr(inflight, "trade_type", None) != order.trade_type):
                    # 反方向大单不拆不排队：单笔直提，保风控退出时效
                    # （docstring 承诺：SELL 不冻结现金无超预算问题）
                    self._rec_live(
                        "split_order", "skip", portfolio, order,
                        message="反方向拆单队列在途：本单不拆不排队，单笔直提（保风控退出时效）",
                    )
                    return False
            # 父单也打「该 bar 已处理」标记（seq=0）：重启重驱同一根 bar 在闸门处即被
            # 拦住，不会重新拆单排队。并发撞键（另一线程已处理）→ 不建队列、原单不提交。
            # **必须写在反方向旁路之后**：旁路 return False 会让原单继续走正常提交链，
            # 父单标记若先落地并 commit，原单就在 _submit_one_order 的同键幂等处被回滚
            # （SELL 单凭空消失，2026-11-11 修）。
            if not self._mark_bar_processed(db, order, seq=0):
                logger.info(
                    "split aborted: bar already processed %s bar=%s (session %s)",
                    order.stock_code, order.bar_time, self.session_id,
                )
                self._rec_live(
                    "bar_processed", "block", portfolio, order,
                    message="跳过：该 bar 已处理（拆单父单并发重复，不建队列/不提交）",
                )
                return True
            # 立即持久化：拆单排队分支不会再提交任何订单行，标记必须自己落地，
            # 否则会话回滚/重启后就丢了这道门。
            db.commit()
            if entry is not None:
                # 同策略同票已有队列：新单子单排队等待当前队列完成，不并行提交
                self._rec_live(
                    "split_order", "split", portfolio, order, message=split_msg,
                )
                entry["pending"].extend(children)
                self._rec_live(
                    "split_order", "split", portfolio, order,
                    message="已有拆单队列在途：本单 %d 笔子单排队等待依次提交"
                            % len(children),
                )
                return True
            self._rec_live(
                "split_order", "split", portfolio, order, message=split_msg,
            )
            self._split_children[key] = {
                "inflight": children[0],
                "pending": children[1:],
                "portfolio": portfolio,
                "period": period,
            }
            # 子单 1 立即提交（走完整闸门链）。提交返回时落库行必已 commit
            # （submitted 或 rejected），deals 池据行状态续发/中止。
            self._submit_one_order(db, portfolio, children[0], period,
                                   emit_signal=False)
            if self._find_split_child_row(db, children[0]) is None:
                # 防御：子单 1 被前置闸门吞掉且未落库（审批/deferred 等）→ 不建队列
                self._split_children.pop(key, None)
                self._rec_live(
                    "split_order", "abort", portfolio, order,
                    message="拆单中止：子单 1 未受理（前置闸门拦截），剩余 %d 笔不提交"
                            % (n - 1),
                )
        return True

    def _find_split_child_row(self, db: Session,
                              child: Optional[OrderEvent]) -> Optional[LiveOrder]:
        """子单 OrderEvent → LiveOrder 落库行。不过滤状态（终态也要能查到）。

        子单 bar_time 毫秒级互异 + 原单不落库，(session, 组合, 策略, 股票, 方向,
        bar_time) 精确唯一定位本子单。"""
        if child is None:
            return None
        op = "buy" if child.trade_type == TradeType.BUY else "sell"
        return (
            db.query(LiveOrder)
            .filter(
                LiveOrder.live_session_id == self.session_id,
                LiveOrder.portfolio_strategy_id == child.portfolio_id,
                LiveOrder.strategy_id == child.strategy_id,
                LiveOrder.stock_code == child.stock_code,
                LiveOrder.trade_type == op,
                LiveOrder.bar_time == child.bar_time,
            )
            .order_by(LiveOrder.id.desc())
            .first()
        )

    def _tick_split_children(self) -> None:
        """批次2：拆单队列成交驱动续发（_tick_deals 回填后调用，持 _book_lock）。

        遍历队列：队首（inflight）子单 filled → pop 并提交下一笔（走
        _submit_one_order 完整闸门链，独立审批）；pending 已空 → complete 决策
        事件 + 清队。队首子单 canceled/rejected（超时撤单/柜台废单/桥拒单）→
        整队丢弃 + abort 决策事件（message 含剩余数量）。submitted/partial →
        等下轮回填。"""
        if not self._split_children:
            return
        db = self._db_session_factory()
        try:
            first_port: Optional[Portfolio] = None
            for key in list(self._split_children.keys()):
                entry = self._split_children.get(key)
                if entry is None:
                    continue
                if first_port is None:
                    first_port = entry.get("portfolio")
                self._advance_split_queue(db, key, entry)
            self._drain_decisions(db, first_port)
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _advance_split_queue(self, db: Session, key: tuple, entry: dict) -> None:
        """推进单个拆单队列（持 _book_lock 调用）：续发 / 完成 / 中止 / 等待。"""
        portfolio = entry.get("portfolio")
        inflight: Optional[OrderEvent] = entry.get("inflight")
        if portfolio is None or inflight is None:
            self._split_children.pop(key, None)  # 组合/子单缺失（理论不可达）：清队
            return
        row = self._find_split_child_row(db, inflight)
        pending: List[OrderEvent] = entry.get("pending") or []
        if row is None:
            # 防御：在途子单无落库行（正常路径子单落库后才建队）→ 视同废单中止
            self._abort_split_queue(key, portfolio, inflight, 0, pending,
                                    reason="在途子单无落库记录")
            return
        if row.status == "filled":
            if not pending:
                self._rec_live(
                    "split_order", "complete", portfolio, inflight,
                    message="拆单队列完成：全部子单已成交",
                )
                self._split_children.pop(key, None)
                return
            nxt = pending.pop(0)
            entry["inflight"] = nxt
            entry["pending"] = pending
            self._submit_one_order(db, portfolio, nxt, entry.get("period"),
                                   emit_signal=False)
            if self._find_split_child_row(db, nxt) is None:
                # 续发子单未落库（审批/前置闸门拦截）→ 中止剩余，防队列空转
                self._abort_split_queue(key, portfolio, nxt, 0, pending,
                                        reason="续发子单未受理（前置闸门拦截）")
            return
        if row.status in ("canceled", "rejected"):
            self._abort_split_queue(
                key, portfolio, inflight,
                int(row.quantity or 0) - int(row.filled_quantity or 0), pending,
                reason="子单%s（%s）" % (
                    "已撤单" if row.status == "canceled" else "已废单/被拒",
                    (row.error_message or row.status or "")[:80],
                ),
            )
        # submitted/partial：等下轮回填

    def _abort_split_queue(self, key: tuple, portfolio: Portfolio,
                           order: OrderEvent, unfilled: int,
                           pending: List[OrderEvent], reason: str) -> None:
        """整队中止：abort 决策事件（中文含剩余数量）+ 清队。剩余 = 在途子单
        未成交量 + 全部未提交子单量（已成交部分保留账面，不回滚）。"""
        remaining = max(0, unfilled) + sum(c.quantity for c in pending)
        self._rec_live(
            "split_order", "abort", portfolio, order,
            message="拆单中止：%s，丢弃剩余 %d 股（未提交 %d 笔）"
                    % (reason, remaining, len(pending)),
        )
        logger.warning(
            "split queue aborted (session %s) key=%s: %s, remaining=%d",
            self.session_id, key, reason, remaining,
        )
        self._split_children.pop(key, None)

    def _note_minute_bar_date(self, bar: BarEvent) -> None:
        """记录最近一次收到的分钟级 bar 交易日（日期推进时同步记上一值）。

        `_prev_minute_bar_date` 只在日期推进时更新，因此同一根 bar 驱动多个组合时
        顺延判定一致（不会因先处理的组合推进了日期而改变后处理组合的判定）。
        """
        if bar is None or bar.bar_time is None:
            return
        if getattr(bar, "period", None) in self._DAILY_PERIODS:
            return
        d = bar.bar_time.date()
        if self._last_minute_bar_date is None or d > self._last_minute_bar_date:
            self._prev_minute_bar_date = self._last_minute_bar_date
            self._last_minute_bar_date = d

    # 掘金桥 1m 放行探测的硬预算：单轮最多占用主循环的秒数（实测单只 ~5s）
    _BRIDGE_PROBE_BUDGET_SECONDS = 15.0
    # 预留的最小单次调用预算：剩余不足即放弃本轮（单次调用最坏 = HTTP 10s 超时）
    _BRIDGE_PROBE_MIN_SLICE_SECONDS = 5.0
    # 探测失败 / 拿不到今日完成 bar 后的退避时长（分钟）
    _BRIDGE_PROBE_BACKOFF_MINUTES = 5

    def _maybe_bridge_deferred_release(self, now: datetime) -> None:
        """掘金通道：桥 /quote 的「今日已完成 1m bar」→ 立即放行 deferred。

        定位：默认行情源仍是通达信快照合成，桥数据只做**放行闸**辅助，成本必须
        接近零——只探真正挂着「跨日未放行」deferred 的 code（无单不探）、已借桥
        放行过的 code 本会话不再探、失败/无当日完成 bar 退避 5 分钟、单轮硬预算
        15s（超时放弃并 WARNING，绝不把 60s 主循环拖住）。

        背景：goldminer 的 1m 驱动行情走快照合成（gm history_n 单只 ~5s 云端往返，
        150 只一轮 ~12 分钟，不能当 1m 轮询源）；合成器启动时没有历史，最早要等
        启动后的下一分钟边界才有首根完成 bar（开盘约 09:31、午休/盘中重启更晚），
        昨日 deferred 日线单因此迟迟不放行。

        数值不混用：只用桥 bar 的日期/时间判定「今日已有完成 bar」，构造
        baseline=True 的 BarEvent 交给 _on_bar——baseline 只放行 deferred + 走提交
        链，不跑公式/风控评估，也不改动快照 bar 流。

        跳过：非掘金通道 / alert 模式 / 09:31 前 / 无跨日 deferred / 退避期内。
        桥不可达或返回空 → WARNING 后返回：不放行、不清单，等快照首根完成 bar
        兜底（与桥断时 iquant 通道的行为一致，绝不静默切回另一套 bar）。
        """
        if self.execution_mode == "alert" or str(self._engine_key or "") != "goldminer":
            return
        if (now.hour, now.minute) < (9, 31):
            return
        next_at = self._bridge_probe_next_at
        if next_at is not None and now < next_at:
            return
        today = now.date()
        pending = [
            o for o in self._signal_bar_deferred
            if o.bar_time is not None and o.bar_time.date() < today
            and o.stock_code not in self._bridge_probe_released_codes
        ]
        if not pending:
            return
        codes: List[str] = []
        for o in pending:
            if o.stock_code not in codes:
                codes.append(o.stock_code)
        retry_at = now + timedelta(minutes=self._BRIDGE_PROBE_BACKOFF_MINUTES)
        deadline = time.monotonic() + self._BRIDGE_PROBE_BUDGET_SECONDS
        for code in codes:
            if deadline - time.monotonic() < self._BRIDGE_PROBE_MIN_SLICE_SECONDS:
                logger.warning(
                    "goldminer 桥 1m 放行探测超出单轮预算 %.0fs，放弃本轮并退避 %d 分钟"
                    "（session %s）",
                    self._BRIDGE_PROBE_BUDGET_SECONDS,
                    self._BRIDGE_PROBE_BACKOFF_MINUTES, self.session_id,
                )
                self._bridge_probe_next_at = retry_at
                return
            try:
                bars = self._dispatcher.query_quote(code, period="1m", count=3)
            except Exception as e:  # noqa: BLE001 - 桥不可达及其它异常同一处置
                logger.warning(
                    "goldminer 桥 1m 放行探测失败，%d 分钟内不再探测、等快照首根完成 bar："
                    "%s（session %s）",
                    self._BRIDGE_PROBE_BACKOFF_MINUTES, e, self.session_id,
                )
                self._bridge_probe_next_at = retry_at
                return
            cb = latest_completed_bar(bars)
            bt = parse_bar_time(cb) if cb is not None else None
            if bt is None or bt.date() != today:
                continue  # 该只今日尚无完成 bar（停牌/刚开盘）→ 试下一只
            self._bridge_probe_released_codes.add(code)
            logger.info(
                "goldminer 桥 1m 已完成 bar 放行 deferred：bar=%s code=%s 待放行单=%d（session %s）",
                bt.strftime("%Y-%m-%d %H:%M"), code, len(pending), self.session_id,
            )
            self._on_bar(BarEvent(
                stocks={code: to_ohlcv(cb)}, bar_time=bt,
                period="1m", baseline=True,
            ))
            return
        # 全部候选 code 都没有今日完成 bar：退避后再试（不连打云端）
        self._bridge_probe_next_at = retry_at

    def _release_signal_bar_deferred(
        self, portfolio: Portfolio, bar: BarEvent
    ) -> List[OrderEvent]:
        """B-1：跨日放行 deferred 的日线信号订单（引擎级队列，只取本组合的）。

        放行条件与回测闸门同源：order.bar_time.date() < 当前 bar 日期（严格小于
        =已跨日，同日后续 bar 不放行）。放行时把 order.bar_time 更新为当前 bar
        时间（决策时间戳语义），随后重走完整提交链。

        过期规则（2026-11-09）：只允许在「signal_bar_date 的下一交易日」放行。
        当前 bar 日期已跨过该交易日（长假停跑/漏开机/长跑进程跳过交易日）→
        置 canceled + 决策事件，绝不拿过期信号追单。

        P1 顺延（2026-11-09）：若 due 当日引擎一根分钟级 bar 都没收到（行情链路
        故障/快照合成停摆），说明「没机会放行」而非「信号过期」→ 顺延一个交易日
        放行一次。DB 行 error_message 记 ``deferred_extended:due=<原due>``（重启
        recover 读回），第二个到期日仍无机会才正常取消，绝不无限顺延。
        """
        if not self._signal_bar_deferred:
            return []
        # P0（2026-11-09）：只允许**分钟级** bar 放行 deferred。
        # 收盘后启动时，15:05 日线级别轮次会先于次日首根分钟 bar 到达——旧实现把
        # 它当放行 bar：deferred 单被真实报出（收盘后必成废单/待报单），且 bar_time
        # 被改写成当日 00:00，令 15:00 收盘守卫（按 bar_time 判定）失效。日线及以上
        # 周期与收盘落定轮次一律不放行，等次一交易日首根分钟 bar。
        if (getattr(bar, "period", None) in self._DAILY_PERIODS
                or getattr(bar, "post_close", False)):
            return []
        if bar.bar_time is None:
            return []
        bar_date = bar.bar_time.date()
        released: List[OrderEvent] = []
        remaining: List[OrderEvent] = []
        for order in self._signal_bar_deferred:
            if not (order.portfolio_id == portfolio.portfolio_id
                    and order.bar_time is not None
                    and order.bar_time.date() < bar_date):
                remaining.append(order)
                continue
            signal_day = order.signal_bar_date
            due = self._next_trading_day(signal_day) if signal_day is not None else None
            grace_days = 0
            if due is not None:
                # 只在日历 fail-open 的年份放宽：fail-open 把长假中的工作日误算成
                # 交易日 → due 偏早，节前单会在节后首个真实交易日被判过期作废。
                # 权威/内置日历年份保持「一交易日即过期」的严格语义。
                try:
                    if self._calendar.is_fail_open(bar_date):
                        grace_days = _DEFERRED_RELEASE_GRACE_DAYS
                except AttributeError:  # 测试用轻量日历无此方法
                    grace_days = 0
                # 收盘后才启动（due 当日的放行窗口已过）：允许顺延到次一交易日首根
                # 分钟 bar 放行，而不是把上一交易日的待执行单直接作废。日线/收盘
                # 落定轮次仍不放行（上方守卫），所以不会出现「收盘后报单」。
                started = getattr(self, "_started_at", None)
                if grace_days == 0 and started is not None:
                    missed = (started.date() > due or (
                        started.date() == due
                        and (started.hour, started.minute) >= _MARKET_CLOSE_TIME))
                    if missed:
                        grace_days = 1
            extended = bool(getattr(order, "_deferred_extended", False))
            if due is not None and extended:
                # 已顺延过一次：到期日 = 原 due 的下一交易日，再超即取消（不再顺延）
                due = self._next_trading_day(due)
            if (due is not None
                    and bar_date > due + timedelta(days=grace_days)):
                # P1：due 当日一根分钟 bar 都没收到（行情失效）→ 顺延一个交易日。
                # 「bar 日期恰好是 due 的下一交易日」给顺延幅度封顶，停摆多日不会
                # 拿过期信号追单；_prev_minute_bar_date 为本次 bar 之前引擎收到的
                # 最后一根分钟 bar 日期（None=从没收到过）。
                no_bar_on_due = (self._prev_minute_bar_date is None
                                 or self._prev_minute_bar_date < due)
                if (not extended and grace_days == 0 and no_bar_on_due
                        and bar_date == self._next_trading_day(due)):
                    self._extend_deferred_order(order, portfolio, due)
                else:
                    self._expire_deferred_order(order, portfolio, bar_date, due)
                    continue
            order.bar_time = bar.bar_time
            # 价格刷新（P4）：默认用**当日首根 1m bar 开盘价**做限价（对齐回测
            # 「次日开盘成交」，可复现）；取不到首根 open 才退回实时 close 并记
            # fallback 事件。保证成交开关开启时走通道对手价。
            self._refresh_release_price(order)
            released.append(order)
        self._signal_bar_deferred = remaining
        return released

    def _extend_deferred_order(
        self, order: OrderEvent, portfolio: Portfolio, due: date
    ) -> None:
        """P1：due 当日无任何分钟 bar → 顺延一个交易日放行（只一次）。

        顺延标记写 DB 行 error_message（``deferred_extended:due=<原due>``），
        重启 recover 时读回 ``_deferred_extended``，第二个到期日正常取消。
        """
        order._deferred_extended = True
        msg = ("deferred 顺延：signal_bar_date=%s 应于 %s 放行，但该日未收到任何"
               "分钟级行情（行情链路/快照合成故障），顺延一个交易日执行（仅一次）"
               % (order.signal_bar_date, due.isoformat()))
        logger.warning(
            "extend deferred %s %s qty=%s signal_bar_date=%s due=%s "
            "(due 当日无分钟 bar，顺延一交易日) (session %s)",
            order.trade_type.value, order.stock_code, order.quantity,
            order.signal_bar_date, due, self.session_id,
        )
        self._rec_live("signal_bar_date", "extend", portfolio, order, message=msg)
        self._emit_risk_safe({
            "session_id": self.session_id,
            "rule": "deferred_extended",
            "portfolio_id": portfolio.portfolio_id,
            "strategy_id": order.strategy_id,
            "stock_code": order.stock_code,
            "trade_type": order.trade_type.value,
            "quantity": int(order.quantity),
            "price": float(order.price) if order.price is not None else None,
            "signal_bar_date": str(order.signal_bar_date),
            "due": due.isoformat(),
            "message": msg,
        })
        self._alert_message_safe("【实盘】%s" % msg)
        row_id = getattr(order, "_deferred_row_id", None)
        if not row_id or self._db_session_factory is None:
            return
        db = self._db_session_factory()
        try:
            row = db.query(LiveOrder).filter(LiveOrder.id == row_id).first()
            if row is not None:
                marker = "deferred_extended:due=%s" % due.isoformat()
                row.error_message = (
                    ((row.error_message + "; ") if row.error_message else "") + marker
                )[:500]
                db.commit()
        except Exception:  # noqa: BLE001
            db.rollback()
            logger.exception(
                "extend deferred row update failed id=%s (session %s)",
                row_id, self.session_id)
        finally:
            db.close()

    def _emit_risk_safe(self, payload: dict) -> None:
        """risk 事件投递（观测链路）：失败只记日志，绝不影响放行/过期主链路。"""
        try:
            self._emit("risk", payload)
        except Exception:  # noqa: BLE001
            logger.warning(
                "emit risk event failed rule=%s (session %s)",
                payload.get("rule"), self.session_id, exc_info=True)

    def _alert_message_safe(self, msg: str) -> None:
        """TQ 告警通道（可选）：未启用/发送失败都不影响主链路。"""
        alert = getattr(self, "_tq_alert", None)
        send = getattr(alert, "send_message", None) if alert is not None else None
        if send is None:
            return
        try:
            send(msg)
        except Exception:  # noqa: BLE001
            logger.warning(
                "tq alert send failed (session %s)", self.session_id, exc_info=True)

    def _expire_deferred_order(
        self, order: OrderEvent, portfolio: Portfolio, bar_date: date, due: date
    ) -> None:
        """deferred 过期：cancel + 决策事件 + risk 事件 +（若有行）DB 行置 canceled。

        放行窗口 = 「signal_bar_date 的下一交易日」（顺延过一次则为顺延后的到期日）；
        跨过即过期。DB 行保留（改 canceled 留审计），之后不再进入内存队列，不会在
        后续重启中被反复恢复。
        """
        msg = ("deferred 过期：signal_bar_date=%s 应于 %s 放行，当前 bar 日期 "
               "%s 已超过放行窗口，取消不下单"
               % (order.signal_bar_date, due.isoformat(), bar_date.isoformat()))
        logger.warning(
            "expire deferred %s %s qty=%s signal_bar_date=%s due=%s now=%s "
            "(session %s)",
            order.trade_type.value, order.stock_code, order.quantity,
            order.signal_bar_date, due, bar_date, self.session_id,
        )
        self._rec_live(
            "signal_bar_date", "expire", portfolio, order, message=msg,
        )
        self._emit_risk_safe({
            "session_id": self.session_id,
            "rule": "deferred_expired",
            "portfolio_id": portfolio.portfolio_id,
            "strategy_id": order.strategy_id,
            "stock_code": order.stock_code,
            "trade_type": order.trade_type.value,
            "quantity": int(order.quantity),
            "price": float(order.price) if order.price is not None else None,
            "signal_bar_date": str(order.signal_bar_date),
            "due": due.isoformat(),
            "bar_date": bar_date.isoformat(),
            "extended": bool(getattr(order, "_deferred_extended", False)),
            "message": msg,
        })
        self._alert_message_safe("【实盘】%s" % msg)
        row_id = getattr(order, "_deferred_row_id", None)
        if not row_id or self._db_session_factory is None:
            return
        db = self._db_session_factory()
        try:
            row = db.query(LiveOrder).filter(LiveOrder.id == row_id).first()
            if row is not None:
                row.status = "canceled"
                row.error_message = (
                    "stale_release: deferred 过期（signal_bar_date=%s，应于 %s 放行，"
                    "当前 bar 日期 %s）"
                    % (order.signal_bar_date, due, bar_date)
                )[:500]
                db.commit()
            order._deferred_row_id = None
        except Exception:  # noqa: BLE001
            db.rollback()
            logger.exception(
                "expire deferred row update failed id=%s (session %s)",
                row_id, self.session_id)
        finally:
            db.close()

    def _requeue_rejected_order(
        self, db: Session, portfolio: Portfolio, order: OrderEvent,
        live_order: LiveOrder, reason: str,
    ) -> bool:
        """P1-3：把可重排拒单写回 deferred 行并放回内存队列。

        返回 True = 已重排（调用方直接 return，不判终态）；False = 次数用尽，
        交调用方走原终态 rejected 分支。

        语义边界（与 at-most-once 不冲突）：
        - 重排不是「当日重报」——bar_time 保持本次已尝试的 bar，`signal_bar_date`
          对齐该日（recover 同口径），下次放行只能发生在**次一交易日**的 due 窗口
          （日线）或下一根 bar（分钟，due=None 时按 `bar_time.date() < bar_date`）；
        - 次数随 `error_message` 的 deferred_retry:n= 标记落库，重启 recover 读回，
          不会因重启把上限刷回 0；
        - 行保持 deferred（不新建行），保证「同一逻辑单只有一行足迹」。
        """
        count = int(getattr(order, "_deferred_retry_count", 0) or 0)
        if count >= _DEFERRED_RETRY_MAX:
            return False
        count += 1
        order._deferred_retry_count = count
        live_order.status = "deferred"
        live_order.error_message = ("%s%d; %s" % (_RETRY_MARKER, count, reason))[:500]
        # 重排 = 下一交易日 due 窗口：signal_bar_date 对齐**原信号 bar 日**（盘前单存
        # 在 _pre_open_bar_time；其余取当前 bar_time），与 recover 同口径；否则日线单
        # 会在次日直接落「过期取消」分支。
        retry_bar_time = getattr(order, "_pre_open_bar_time", None) or order.bar_time
        if retry_bar_time is not None:
            order.signal_bar_date = retry_bar_time.date()
            if getattr(order, "_pre_open_bar_time", None) is not None:
                # DB 行同步回原信号 bar 日（recover 才能按 due=今日 重建）
                live_order.bar_time = retry_bar_time
        order._deferred_row_id = live_order.id
        order._requeued_for_retry = True
        with self._book_lock:
            self._pending_orders.pop(live_order.id, None)
            if all(id(o) != id(order) for o in self._signal_bar_deferred):
                self._signal_bar_deferred.append(order)
            self._emit("order", {
                "portfolio_id": portfolio.portfolio_id,
                "order_id": live_order.id,
                "strategy_id": live_order.strategy_id,
                "stock_code": live_order.stock_code,
                "trade_type": (live_order.trade_type or "").upper(),
                "status": "deferred",
                "quantity": live_order.quantity,
                "price": float(live_order.price) if live_order.price is not None else None,
                "bar_time": live_order.bar_time.isoformat() if live_order.bar_time else None,
                "error_message": live_order.error_message,
            })
            db.commit()
        logger.warning(
            "[reject-requeue] %s %s 第 %d/%d 次尝试被拒 → 回 deferred，等下一窗口重试: %s "
            "(session %s)", order.trade_type.value, order.stock_code,
            count + 1, _DEFERRED_RETRY_MAX + 1, reason, self.session_id)
        self._rec_live(
            "bridge_rejected", "requeue", portfolio, order,
            message=("可重排拒单（%s）：回 deferred，次一交易日 due 窗口/下一根 bar 重试"
                     "（第 %d/%d 次尝试；上限 %d 次重排）"
                     % (reason, count + 1, _DEFERRED_RETRY_MAX + 1, _DEFERRED_RETRY_MAX)),
        )
        self._emit("risk", {
            "session_id": self.session_id,
            "engine": self._engine_key,
            "gate": "bridge_rejected",
            "action": "requeue",
            "rule": "order_requeued",
            "code": order.stock_code,
            "side": order.trade_type.value,
            "attempt": count + 1,
            "max_attempts": _DEFERRED_RETRY_MAX + 1,
            "message": "可重排拒单：%s（回 deferred 重试，不是终态）" % reason,
        })
        return True

    def _settle_unsubmitted_released(
        self, db: Session, portfolio: Portfolio, orders: List[OrderEvent]
    ) -> None:
        """放行但未提交的 deferred 单收尾：DB 行 deferred → canceled（终态）。

        放行后单已离开内存队列；若被资金/仓位/交易时段/去重等闸门拦下而不收尾，
        该行会一直以 deferred 留在库里，下次重启被再次恢复 → 永远重试的脏足迹。
        「未提交 = 未执行」，故记 canceled + 决策事件（原因见消息）。

        判据：_submit_one_order 只有在真正落 submitted 时才会清空
        order._deferred_row_id；仍带行 id = 本次未提交。

        P1-3 例外：被桥明确拒单/价格闸拦截后**已重排**（`_requeued_for_retry`）
        的单行已是 deferred 且已回队，绝不能在这里改 canceled（否则重试被吞）。
        """
        for order in orders:
            if getattr(order, "_requeued_for_retry", False):
                continue
            row_id = getattr(order, "_deferred_row_id", None)
            if not row_id:
                continue
            logger.warning(
                "released deferred %s %s not submitted, mark canceled (row %s)",
                order.trade_type.value, order.stock_code, row_id)
            self._rec_live(
                "release", "cancel", portfolio, order,
                message=("放行后未提交（资金/仓位/交易时段/去重等闸门拦截），"
                         "取消原 deferred 单，避免重启后重复恢复"),
            )
            row = db.query(LiveOrder).filter(LiveOrder.id == row_id).first()
            if row is not None:
                row.status = "canceled"
                row.error_message = (
                    "released_not_submitted: 放行后未提交（闸门拦截），转 canceled"
                )[:500]
            order._deferred_row_id = None

    def _find_duplicate_deferred_row(
        self, db: Session, order: OrderEvent
    ) -> Optional[LiveOrder]:
        """A 源头幂等：查同键已存在的 deferred 落库行（无则返回 None）。

        键 = (组合, 策略, 股票, 方向, signal_bar_date, 数量)。相对需求给的 4 元组
        (portfolio_strategy_id, stock_code, trade_type, signal_bar_date) 多带
        strategy_id 与 quantity，理由是存在合法的同日同向多笔：
        - strategy_id：同一组合内多策略可各持同一只票，同日各自触发止损/减仓
          （positions 按策略独立维护）。4 元组会把第二笔合法卖单误判为重复丢弃，
          导致该策略持仓卖不掉；
        - quantity：同策略同日分批减仓（reduce_ratio 分档止盈）需要多笔，仅按方向
          去重会吞掉后一档。
        重启重复驱动产生的是字段全同的同一逻辑单，仍会被拦下（2026-11-04 的 6 条
       重复卖单即 portfolio/strategy/代码/方向/日期/数量全同）。

        deferred 落库时 signal_bar_date 恒等于 bar_time 当日（defer 闸门定义），
        故用 bar_time 当日区间查。
        """
        d = order.signal_bar_date
        if d is None:
            return None
        start = datetime.combine(d, datetime.min.time())
        return (
            db.query(LiveOrder)
            .filter(
                LiveOrder.live_session_id == self.session_id,
                LiveOrder.status == "deferred",
                LiveOrder.portfolio_strategy_id == order.portfolio_id,
                LiveOrder.strategy_id == order.strategy_id,
                LiveOrder.stock_code == order.stock_code,
                LiveOrder.trade_type == order.trade_type.value.lower(),
                LiveOrder.quantity == order.quantity,
                LiveOrder.bar_time >= start,
                LiveOrder.bar_time < start + timedelta(days=1),
            )
            .order_by(LiveOrder.id)
            .first()
        )

    def _persist_deferred_queue_on_stop(self) -> int:
        """stop() 兜底：把内存 deferred 队列逐单确保有 status='deferred' 的 DB 行。

        - 行 id 存在且该行仍是 deferred → 不动（最常见：defer 时已落库）；
        - 无行 id / 行已被删（提交失败回滚、旧版本内存单）→ 先按 A 幂等键查重，
          命中则接管该行 id，未命中才补插一行。
        返回「最终有 DB 行」的条数，供 stop 日志如实汇报（不再无条件写 kept in DB）。
        任一单失败只告警，不阻断 stop（队列仍会清空，但已尽量落库）。
        """
        orders = list(self._signal_bar_deferred)
        if not orders or self._db_session_factory is None:
            return 0
        db = self._db_session_factory()
        kept = 0
        try:
            for order in orders:
                row_id = getattr(order, "_deferred_row_id", None)
                if row_id:
                    exists = (
                        db.query(LiveOrder.id)
                        .filter(LiveOrder.id == row_id,
                                LiveOrder.status == "deferred")
                        .first()
                    )
                    if exists is not None:
                        kept += 1
                        continue
                try:
                    dup = self._find_duplicate_deferred_row(db, order)
                    if dup is not None:
                        order._deferred_row_id = dup.id
                        kept += 1
                        continue
                    lo = self._persist_order_submitted(db, order, status="deferred")
                    db.flush()
                    order._deferred_row_id = lo.id
                    kept += 1
                except Exception:  # noqa: BLE001
                    db.rollback()
                    logger.exception(
                        "stop: re-persist deferred %s %s failed (session %s)",
                        order.trade_type.value, order.stock_code, self.session_id)
            db.commit()
        finally:
            db.close()
        return kept

    def _exposure_kwargs(self, portfolio: Portfolio, ctx) -> dict:
        """B-2 策略级敞口上限（#3，与 BacktestEngine._exposure_kwargs 完全同式）。

        enabled 判定 exposure_cap_enabled；策略持仓市值口径 = portfolio.holdings_value(ctx)
        （该策略虚拟持仓按最近已知价估值，_handle_bar 顶部 total_value(bar) 已刷新
        价格快照，与回测「上一 bar 收盘估值」同口径）；上限 = capital_ratio × 组合
        初始资金。关闭返回空 dict，cap_quantity/execute 走默认 None（行为不变）。

        同时携带策略级单笔最大买入金额（可选 None=不限，与回测同式透传
        cap_quantity，双端生效）。
        """
        kwargs = {}
        if getattr(ctx, "exposure_cap_enabled", False):
            kwargs["strategy_position_value"] = portfolio.holdings_value(ctx)
            kwargs["strategy_capital_limit"] = (
                ctx.capital_ratio * portfolio.account.initial_capital
            )
        _moa = getattr(ctx, "max_order_amount", None)
        kwargs["max_order_amount"] = (
            Decimal(str(_moa)) if _moa is not None else None
        )
        return kwargs

    @staticmethod
    def _buy_fee_cfg(portfolio: Portfolio) -> Optional[tuple]:
        """Fix4：组合费用参数 → execute 费用感知钳制口径 (最低佣金, 买入费率)。

        cost_params 缺项（理论不可达，assemble_portfolio 恒全量装配）→ None，
        execute 跳过前置钳制（原行为）。
        """
        cp = getattr(portfolio, "cost_params", None) or {}
        min_comm = cp.get("min_commission")
        rate = cp.get("buy_commission_rate")
        if min_comm is None or rate is None:
            return None
        return (min_comm, rate)

    def _rec_live(
        self, gate: str, action: str, portfolio: Portfolio, order: OrderEvent,
        message: Optional[str] = None,
    ) -> None:
        """记录一条实盘专属闸门事件（收盘/日历/去重/在途/桥拒单）。"""
        self._recorder.record(
            gate=gate, layer="live_gate", action=action,
            portfolio_id=portfolio.portfolio_id,
            strategy_id=order.strategy_id,
            stock_code=order.stock_code,
            bar_time=order.bar_time,
            message=message,
        )

    def _drain_decisions(self, db, portfolio: Portfolio) -> None:
        """把本 bar 缓冲的决策闸门事件落 LiveDecisionEvent + 推 SSE，一次 commit。

        同一 (bar_time, 组合, 策略, 票, 闸门, 动作, 参数) 只保留首条——daily_closer
        用同一份日线快照依次驱动 1d/1w/1mon，风控会在每个周期重跑一遍，事件级去重
        避免同一决策被落库/推送三轮（去重只作用于观测链路，不影响交易本身）。
        失败不影响交易主链路（仅观测）：回滚本批事件行、记异常，不抛出。
        """
        events = self._recorder.drain()
        if not events:
            return
        try:
            for ev in events:
                key = None
                if ev.bar_time is not None:
                    key = (
                        ev.bar_time, ev.portfolio_id, ev.strategy_id,
                        ev.stock_code, ev.gate, ev.action, ev.param_name,
                    )
                if not self._first_emit_for_key(key):
                    continue
                db.add(LiveDecisionEvent(
                    live_session_id=self.session_id,
                    gate=ev.gate,
                    layer=ev.layer,
                    action=ev.action,
                    portfolio_id=ev.portfolio_id,
                    strategy_id=ev.strategy_id,
                    stock_code=ev.stock_code,
                    bar_time=ev.bar_time,
                    param_name=ev.param_name,
                    param_value=ev.param_value,
                    actual_value=ev.actual_value,
                    requested_qty=ev.requested_qty,
                    final_qty=ev.final_qty,
                    message=(ev.message or "")[:200],
                ))
                self._emit("decision", {
                    "portfolio_id": ev.portfolio_id,
                    "strategy_id": ev.strategy_id,
                    "stock_code": ev.stock_code,
                    "gate": ev.gate,
                    "layer": ev.layer,
                    "action": ev.action,
                    "param_name": ev.param_name,
                    "param_value": ev.param_value,
                    "actual_value": ev.actual_value,
                    "requested_qty": ev.requested_qty,
                    "final_qty": ev.final_qty,
                    "message": ev.message,
                    "bar_time": ev.bar_time.isoformat() if ev.bar_time else None,
                })
            db.commit()
        except Exception:  # noqa: BLE001
            db.rollback()
            logger.exception("drain decision events failed (session %s)", self.session_id)

    def _persist_breaker_count(
        self, portfolio: Portfolio, total_value: Decimal = None
    ) -> None:
        """H4：薄委托 → BreakerService.persist_count（0010 步骤 4）。"""
        self._breaker.persist_count(portfolio, total_value)

    def recover_breaker(self, portfolio_id: int) -> bool:
        """手动恢复某组合熔断（公共方法，薄委托 → BreakerService.recover，0010 步骤 4）。"""
        return self._breaker.recover(portfolio_id)

    def _dispatch_period_bar(self, period: str, boundary_time: datetime) -> str:
        """C6(A) 边界分发（薄委托 → MarketDataService.dispatch_period_bar）。

        返回 "dispatched" / "stale" / "empty"（语义见 MarketDataService 同名方法），
        引擎据此决定 boundary 是否登记、是否挂待补偿。
        """
        return self.market_data.dispatch_period_bar(period, boundary_time)

    # 待补偿边界最长等待：手册 refresh_cache 刷新窗口为 10 分钟（正文）/ 5 分钟（示例
    # 注释），取 15 分钟覆盖一个刷新窗口；超窗后旧边界不再派发（迟到的信号可能已与
    # 当前仓位/价格脱节），只记 WARNING。
    _PENDING_BOUNDARY_TTL = timedelta(minutes=15)

    def _retry_pending_boundaries(self, now_bar_time: datetime) -> None:
        """重试被新鲜度闸拦下的周期边界（拿到当日 bar 才派发，fail-safe 补偿）。

        每根 1m bar 调一次：逐 boundary 逐 period 重跑 _dispatch_period_bar；
        返回 "dispatched" 即从 pending 移除该 period，全部移除后清 boundary。
        跨日残留或超 _PENDING_BOUNDARY_TTL 放弃（旧 bar 不再触发，记 WARNING）。
        BridgeUnavailableError 向上抛（与正常边界分发同口径，由 _on_bar 置离线）。
        """
        if not self._pending_boundaries:
            return
        for bt in list(self._pending_boundaries):
            if (bt.date() != now_bar_time.date()
                    or now_bar_time - bt > self._PENDING_BOUNDARY_TTL):
                logger.warning(
                    "周期边界补偿放弃：boundary=%s periods=%s（跨日或超窗 %d 分钟）",
                    bt, sorted(self._pending_boundaries[bt]),
                    int(self._PENDING_BOUNDARY_TTL.total_seconds() // 60),
                )
                self._pending_boundaries.pop(bt, None)
                continue
            for period in list(self._pending_boundaries[bt]):
                try:
                    status = self._dispatch_period_bar(period, bt)
                except BridgeUnavailableError:
                    raise
                except Exception:  # noqa: BLE001
                    logger.exception("retry %s boundary %s error", period, bt)
                    continue
                if status == "dispatched":
                    self._pending_boundaries[bt].discard(period)
            if not self._pending_boundaries.get(bt):
                self._pending_boundaries.pop(bt, None)

    # ---------------- F5：桥可用持仓（SELL 减仓上限）----------------
    def _refresh_available_map(self) -> None:
        """F5：拉桥 /positions 聚合 m_nCanUseVolume（薄委托 → MarketDataService）。"""
        self.market_data.refresh_available_map()

    # ---------------- 公式信号注入（0010 + C4 #28 三维去重）----------------
    def _fill_signal_cache(
        self,
        portfolio: Portfolio,
        bar: BarEvent,
        bars_by_code: Optional[Dict[str, list]] = None,
        df_cache: Optional[Dict] = None,
        raw_cache: Optional[Dict] = None,
    ) -> None:
        """实盘逐 bar 算公式信号填 signal_cache（薄委托 → MarketDataService）。"""
        self.market_data.fill_signal_cache(
            portfolio, bar, bars_by_code=bars_by_code,
            df_cache=df_cache, raw_cache=raw_cache,
        )

    def _fetch_cached_bars(
        self,
        df_cache: Dict,
        bars_by_code: Optional[Dict[str, list]],
        code: str,
        period: str,
        count: int,
    ) -> list:
        """拉取去重（单 bar 生命周期，薄委托 → MarketDataService）。"""
        return self.market_data._fetch_cached_bars(
            df_cache, bars_by_code, code, period, count
        )

    def _reuse_provided_with_cache(self, code: str, period: str, provided: list, count: int) -> list:
        """BarPoller 透传 bars 并入预热缓存复用（薄委托 → MarketDataService）。"""
        return self.market_data._reuse_provided_with_cache(code, period, provided, count)

    @staticmethod
    def _bars_to_formula_df(bars: list, code: str) -> Optional[dict]:
        """桥 bar dict 列表 → OHLCV DataFrame dict（薄委托 → MarketDataService）。"""
        return MarketDataService._bars_to_formula_df(bars, code)

    @staticmethod
    def _extract_latest_signal(raw: Optional[dict], code: str) -> List[dict]:
        """取公式返回最后一条 bar 的信号（薄委托 → MarketDataService）。"""
        return MarketDataService._extract_latest_signal(raw, code)

    def _inject_startup_periods(self, daily_time: datetime) -> None:
        """C6(C)：1w/1mon 通达信启动注入（薄委托 → MarketDataService）。"""
        self.market_data.inject_startup_periods(daily_time)

    def _startup_periods_missing(self, daily_time: datetime) -> bool:
        """1w/1mon 信号是否全部已预填（薄委托 → MarketDataService）。"""
        return self.market_data.startup_periods_missing(daily_time)

    # ---------------- 订单状态机 + 成交回报回填（委托 OrderStateMachine，0010 步骤 3）----------------
    # 下列方法为薄委托：实现已迁入 core.engine.live.order_machine.OrderStateMachine。
    # 同名方法/静态方法保留于此，既有调用点与测试直连（engine._poll_deals()、
    # engine._backfill_order(...)、engine._match_by_remark(...) 等）穿透到协作者。
    def _persist_order_submitted(self, db: Session, order: OrderEvent,
                                 status: str = "submitted") -> LiveOrder:
        return self._order_machine.persist_order_submitted(db, order, status=status)

    def _try_match_order_ref(self, live_order: LiveOrder, claimed_refs=None,
                             orders=None) -> None:
        self._order_machine.try_match_order_ref(
            live_order, claimed_refs=claimed_refs, orders=orders)

    @staticmethod
    def _match_by_remark(orders, expected_remark, claimed):
        return OrderStateMachine.match_by_remark(orders, expected_remark, claimed)

    @staticmethod
    def _match_legacy_fuzzy(live_order, orders, claimed):
        return OrderStateMachine.match_legacy_fuzzy(live_order, orders, claimed)

    def _poll_deals(self) -> list:
        """薄委托 → OrderStateMachine.poll_deals；返回撤单收敛（canceled）单 id 列表。"""
        return self._order_machine.poll_deals()

    def _sync_terminal_order_status(self, db: Session, pending: list,
                                    orders: Optional[list],
                                    deals: Optional[list]) -> None:
        self._order_machine.sync_terminal_order_status(db, pending, orders, deals)

    def _expire_stale_orders(self, db: Session, pending: Optional[list] = None) -> None:
        self._order_machine.expire_stale_orders(db, pending)

    def _cancel_unfilled_orders(self, db: Session) -> list:
        """A-2（I-4/C-1）：在途单超时未成交 → 真实撤单（薄委托 → OrderStateMachine）。

        返回本轮收敛为 canceled 的单（批次1 3a 追价重下挂点；其余调用方忽略）。
        """
        return self._order_machine.cancel_unfilled_orders(db)

    def _sync_pending_orders(self, db: Session) -> None:
        self._order_machine.sync_pending_orders(db)

    def _backfill_order(self, db: Session, live_order: LiveOrder, matched_deals: list) -> None:
        self._order_machine.backfill_order(db, live_order, matched_deals)

    @staticmethod
    def _parse_trade_time(deal: dict) -> datetime:
        return OrderStateMachine.parse_trade_time(deal)

    def _apply_filled_trade(self, live_order: LiveOrder, price: Decimal,
                            qty: int, amount: Decimal, commission: Decimal) -> None:
        self._order_machine.apply_filled_trade(
            live_order, price, qty, amount, commission)

    # ---------------- 峰值持久化 + 补种（与回测口径对齐，2026-11-12） ----------------
    # 回测峰值 = max(建仓成交价, 建仓后该策略周期每根完成 bar 的 close)，逐根累积、
    # 只升不降。实盘 recover 手里只有成交记录（=买入价），中间 bar 的 close 从未落盘，
    # 峰值会退化为买入价/成本 → 移动止损基准偏低、触发更晚（保护更弱）。
    #
    # 方案 E（阶段二 A/B）：峰值在被抬高时逐 bar UPSERT 到 ``live_position_peaks``，
    # recover **优先读表**（有表值就不再调 TQ）；表里没有才走周期感知补种：
    # ① 表值 → ② 该策略自身周期的 bar close（1m/5m/15m/30m/1h 一律按周期取）
    # → ③ 日线 close → ④ 账户快照 highest_price 历史 MAX → ⑤ 成本（落 fallback 事件）。
    # 全程只 update_high（只升不降）。回测不注入 peak_persist_hook，零影响。
    _PEAK_SEED_FALLBACK_DAYS = 20      # 无锚点时的保守回看窗口（交易日）
    _PEAK_SEED_MAX_BARS = 250          # 日线/未知周期取数上限（约一个交易年）
    _PEAK_TABLE_STALE_DAYS = 30        # live_position_peaks 超期未更新（交易日）→ 清理
    # 周期感知补种的取数根数上限与「每交易日根数」（count 估算用；上限防长区间拉爆）
    _PEAK_SEED_COUNT_BY_PERIOD = {
        "1m": 1200, "5m": 400, "15m": 200, "30m": 150, "1h": 120, "60m": 120,
        "1d": 250, "1w": 120, "1mon": 60,
    }
    _PERIOD_BARS_PER_DAY = {
        "1m": 240, "5m": 48, "15m": 16, "30m": 8, "1h": 4, "60m": 4, "1d": 1,
    }

    def _note_peak_raised(self, portfolio_id: int, code: str, price,
                          bar_time: Optional[datetime] = None) -> None:
        """峰值抬高回调（注入 Portfolio.peak_persist_hook，只实盘用）。

        只在「新值 > 已缓冲值」时更新内存缓冲（不每 bar 都写库）；真正 UPSERT 由
        ``_flush_position_peaks`` 在 bar 收尾/快照采集/recover 时统一执行。任何异常
        都不得冒泡（风控主链路不能被观测性代码打断）。
        """
        try:
            value = Decimal(str(price))
        except Exception:  # noqa: BLE001
            return
        if value <= 0:
            return
        key = (int(portfolio_id), str(code))
        with self._peak_lock:
            prev = self._peak_buffer.get(key)
            if prev is not None and value <= prev[0]:
                return
            self._peak_buffer[key] = (value, bar_time)

    def _load_position_peaks(
            self, db: Session) -> Dict[Tuple[int, str], Tuple[Decimal, Optional[datetime]]]:
        """读本会话的 live_position_peaks → {(组合, 代码): (峰值, 抬峰 bar 时间)}。"""
        try:
            rows = (
                db.query(LivePositionPeak)
                .filter(LivePositionPeak.live_session_id == self.session_id)
                .all()
            )
        except Exception:  # noqa: BLE001 — 表缺失（未迁移）/查询失败：退周期感知补种
            logger.warning("[peak-table] 读取峰值表失败（session %s），走周期补种",
                           self.session_id, exc_info=True)
            return {}
        out: Dict[Tuple[int, str], Tuple[Decimal, Optional[datetime]]] = {}
        for row in rows:
            try:
                price = Decimal(str(row.highest_price))
            except Exception:  # noqa: BLE001
                continue
            if price <= 0:
                continue
            out[(int(row.portfolio_strategy_id), str(row.stock_code))] = (
                price, row.peak_bar_time)
        return out

    def _cleanup_stale_position_peaks(self, db: Session) -> None:
        """清理超过 30 个交易日未更新的峰值行（recover 时一次），记 INFO。

        口径按**交易日**（内置休市表）而非自然日：长假后 30 个交易日仍在持有区间
        内的行不会被误删。行被清理后，下次补种会按策略周期重算，不影响正确性。
        """
        cutoff_day = now_shanghai().date()
        for _ in range(self._PEAK_TABLE_STALE_DAYS):
            cutoff_day = self._prev_trading_day(cutoff_day)
        cutoff = datetime.combine(cutoff_day, datetime.min.time())
        try:
            rows = (
                db.query(LivePositionPeak)
                .filter(
                    LivePositionPeak.live_session_id == self.session_id,
                    LivePositionPeak.updated_at < cutoff,
                )
                .all()
            )
            for row in rows:
                db.delete(row)
        except Exception:  # noqa: BLE001 — 清理失败不阻断起账
            logger.warning("[peak-table] 清理超期峰值行失败（session %s）",
                           self.session_id, exc_info=True)
            return
        if rows:
            logger.info(
                "[peak-table] 清理 %d 条超过 %d 个交易日未更新的峰值行"
                "（session %s，截止 %s）",
                len(rows), self._PEAK_TABLE_STALE_DAYS, self.session_id,
                cutoff_day.isoformat(),
            )

    def _alive_position_keys(self) -> Set[Tuple[int, str]]:
        """当前仍持有的 (组合, 代码) 集合（数量 > 0），清仓删行的判据。"""
        alive: Set[Tuple[int, str]] = set()
        for port in self.ctx.portfolios:
            for ctx in (getattr(port, "strategies", None) or []):
                for code, pos in (getattr(ctx, "positions", None) or {}).items():
                    if pos is not None and int(getattr(pos, "quantity", 0) or 0) > 0:
                        alive.add((int(port.portfolio_id), str(code)))
        return alive

    def _flush_position_peaks(self, db: Optional[Session] = None) -> bool:
        """把缓冲的抬峰 UPSERT 进 live_position_peaks + 清仓删行。

        只写「峰值被抬高」的行（``ON CONFLICT ... WHERE excluded > 当前``），
        绝不写低已有值（只升不降）；清仓（该组合该代码全部策略数量归零）时删行。
        ``db=None`` 时自开短事务并提交；传入会话时只执行不提交（由调用方提交），
        且吞掉异常（观测性链路不得打断交易主链路）。返回是否有写入。
        """
        own = db is None
        with self._peak_lock:
            has_work = bool(self._peak_buffer) or bool(self._peak_rows)
        if own and not has_work:
            return False
        if own:
            db = self._db_session_factory()
        changed = False
        try:
            now = datetime.now(timezone.utc).replace(tzinfo=None)  # naive UTC（§created_at 同口径）
            with self._peak_lock:
                pending = list(self._peak_buffer.items())
                self._peak_buffer.clear()
            for (pid, code), (price, bar_time) in pending:
                stmt = sqlite_insert(LivePositionPeak).values(
                    live_session_id=self.session_id,
                    portfolio_strategy_id=pid,
                    stock_code=code,
                    highest_price=price,
                    peak_bar_time=bar_time,
                    updated_at=now,
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=[
                        "live_session_id", "portfolio_strategy_id", "stock_code"],
                    set_={
                        "highest_price": stmt.excluded.highest_price,
                        "peak_bar_time": stmt.excluded.peak_bar_time,
                        "updated_at": stmt.excluded.updated_at,
                    },
                    where=(
                        LivePositionPeak.highest_price < stmt.excluded.highest_price),
                )
                db.execute(stmt)
                with self._peak_lock:
                    self._peak_rows.add((pid, code))
                changed = True
            # 清仓删行：DB 里存在、但当前没有任何策略持有的 (组合, 代码) 一律删
            alive = self._alive_position_keys()
            rows = (
                db.query(
                    LivePositionPeak.portfolio_strategy_id,
                    LivePositionPeak.stock_code,
                )
                .filter(LivePositionPeak.live_session_id == self.session_id)
                .all()
            )
            for pid, code in rows:
                key = (int(pid), str(code))
                if key in alive:
                    continue
                (
                    db.query(LivePositionPeak)
                    .filter(
                        LivePositionPeak.live_session_id == self.session_id,
                        LivePositionPeak.portfolio_strategy_id == key[0],
                        LivePositionPeak.stock_code == key[1],
                    )
                    .delete(synchronize_session=False)
                )
                with self._peak_lock:
                    self._peak_rows.discard(key)
                changed = True
            if own:
                db.commit()
        except Exception:  # noqa: BLE001 — 落库失败只告警，不影响风控/下单
            if own:
                try:
                    db.rollback()
                except Exception:  # noqa: BLE001
                    pass
            logger.warning("[peak-table] 峰值落库失败（session %s）",
                           self.session_id, exc_info=True)
            return False
        finally:
            if own:
                try:
                    db.close()
                except Exception:  # noqa: BLE001
                    pass
        return changed

    def _peak_anchor_date(self, db: Session, portfolio_id: int, code: str,
                          pos: Position, first_buy_at: Dict[tuple, datetime],
                          today: date) -> Tuple[date, str]:
        """建仓锚点（峰值统计起点）：live_trades 最早 BUY → buy_time/最早快照 → 保守窗口。"""
        t = first_buy_at.get((portfolio_id, code))
        if t is not None:
            return t.date(), "trades"
        bt = getattr(pos, "buy_time", None)
        try:
            first_at = (
                db.query(func.min(LiveAccountSnapshot.captured_at))
                .join(
                    LiveAccountPositionSnapshot,
                    LiveAccountPositionSnapshot.snapshot_id == LiveAccountSnapshot.id,
                )
                .filter(
                    LiveAccountSnapshot.live_session_id == self.session_id,
                    LiveAccountPositionSnapshot.stock_code == code,
                )
                .scalar()
            )
        except Exception:  # noqa: BLE001 — 锚点查询失败只影响窗口精度
            first_at = None
        if bt is not None:
            btd = bt.date()
            # 继承仓（无成交记录）的 buy_time 由 mirror_bridge 写成「导入当日」，不是
            # 真实建仓日；平台更早的账户快照才是首次见到该仓的时间。锚点钉在重启当日
            # 会把快照 MAX / 周期取数区间截断（002745 自愈 10.83 依赖更早的锚点）。
            if isinstance(first_at, datetime) and first_at.date() < btd:
                return first_at.date(), "snapshot_first"
            return btd, "buy_time"
        if isinstance(first_at, datetime):
            return first_at.date(), "snapshot_first"
        anchor = today
        for _ in range(self._PEAK_SEED_FALLBACK_DAYS):
            anchor = self._prev_trading_day(anchor)
        return anchor, "fallback_window"

    def _period_close_peak(self, code: str, period: str, anchor: date,
                           today: date) -> Optional[Decimal]:
        """锚点→今天**指定周期**完成 bar 的 close 最大值（阶段二 B）。

        分钟周期（1m/5m/15m/30m/1h）按策略自身周期取，日线及以上走 1d/1w/1mon；
        取数失败/返回空/通道不支持（query_quote 不存在）→ None（调用方继续降级）。
        只统计锚点当日及之后、今天及之前、close>0 的已完成 bar。
        """
        query_quote = getattr(self._dispatcher, "query_quote", None)
        if query_quote is None:
            return None
        p = (period or "1d").lower()
        cap = self._PEAK_SEED_COUNT_BY_PERIOD.get(p, self._PEAK_SEED_MAX_BARS)
        days = max(1, (today - anchor).days)
        if p == "1d":
            count = max(30, min(cap, days + 10))
        elif p == "1w":
            count = max(20, min(cap, days // 5 + 5))
        elif p == "1mon":
            count = max(12, min(cap, days // 20 + 3))
        else:
            bpd = self._PERIOD_BARS_PER_DAY.get(p, 240)
            count = max(30, min(cap, days * bpd + 10))
        try:
            bars = query_quote(code, period=p, count=count)
        except Exception as e:  # noqa: BLE001 — 桥/TQ 超时、通道不支持都继续降级
            logger.warning("[peak-seed:%s] %s 取数失败（%s: %s）",
                           p, code, type(e).__name__, e)
            return None
        peak: Optional[Decimal] = None
        for b in bars or []:
            bt = parse_bar_time(b)
            if bt is None or bt.date() < anchor or bt.date() > today:
                continue
            try:
                close = to_ohlcv(b).get("close") or Decimal("0")
            except Exception:  # noqa: BLE001
                continue
            if close <= 0:
                continue
            if peak is None or close > peak:
                peak = close
        if peak is None:
            logger.warning("[peak-seed:%s] %s 锚点 %s 以来无可用 bar（返回空）",
                           p, code, anchor.isoformat())
        return peak

    def _daily_close_peak(self, code: str, anchor: date,
                          today: date) -> Optional[Decimal]:
        """日线 close 峰值（保留旧入口；内部转 ``_period_close_peak(period="1d")``）。"""
        return self._period_close_peak(code, "1d", anchor, today)

    def _snapshot_peak_max(self, db: Session, code: str,
                           anchor: date) -> Optional[Decimal]:
        """账户快照里该票 highest_price 的历史 MAX（锚点之后，session+代码）。

        快照表是**会话级**（无 portfolio_strategy_id 列），故按会话+代码聚合。取 MAX
        而不是「最近一条」，正是为了自愈历史上的脏写（实证：峰值曾被写成成本价，
        更早的快照里仍留着真实峰值）。
        """
        try:
            value = (
                db.query(func.max(LiveAccountPositionSnapshot.highest_price))
                .join(
                    LiveAccountSnapshot,
                    LiveAccountPositionSnapshot.snapshot_id == LiveAccountSnapshot.id,
                )
                .filter(
                    LiveAccountSnapshot.live_session_id == self.session_id,
                    LiveAccountPositionSnapshot.stock_code == code,
                    LiveAccountSnapshot.captured_at >= datetime.combine(
                        anchor, datetime.min.time()),
                )
                .scalar()
            )
        except Exception:  # noqa: BLE001 — 快照兜底失败不阻断起账
            logger.warning("[peak-seed:snapshot] %s 快照峰值查询失败（session %s）",
                           code, self.session_id, exc_info=True)
            return None
        if value is None:
            return None
        try:
            peak = Decimal(str(value))
        except Exception:  # noqa: BLE001
            return None
        return peak if peak > 0 else None

    def _seed_position_peaks(self, db: Session,
                             ports_by_id: Dict[int, Portfolio],
                             first_buy_at: Dict[tuple, datetime],
                             skip_keys: Optional[Set[tuple]] = None,
                             ) -> Set[tuple]:
        """recover 峰值补种（阶段二 B）：表值 → 策略周期 → 日线 → 快照 → 成本。

        ① ``live_position_peaks`` 表值**只作下限**：先用本地库查一次快照 MAX
           （零 TQ）。表值 ≥ 快照 MAX → 直接采用，**不调 TQ**（重启零取数）；
           表值 < 快照 MAX → 判为可疑（历史退化值已落表），走 ② 重算并取最大者，
           落 ``gate=peak_seed / action=recompute`` 审计（避免退化值永久固化）；
        ② 用**该策略自身周期**的 bar close 重算（1m/5m/15m/30m/1h 一律如此）；
        ③ 取不到（TQ 超时/历史不足/停牌/通道不支持）→ 降级日线 close；
        ④ 再降级 → 账户快照 highest_price 历史 MAX；
        ⑤ 最后 → 现值（成本/买入价），落 `gate=peak_seed / action=fallback` 事件。
        ``skip_keys``：上一轮已补种的 (组合, 代码)；用于「导入继承仓之后再补一次」
        时去重，避免对已在 live_trades 里的票重复补种、重复取数。
        各候选取 max 后 update_high（只升不降）。返回本轮处理过的键集合。
        """
        skip_keys = skip_keys or set()
        done: Set[tuple] = set()
        targets: List[tuple] = []
        for pid, port in ports_by_id.items():
            for ctx in (getattr(port, "strategies", None) or []):
                for code, pos in (getattr(ctx, "positions", None) or {}).items():
                    if pos is None or int(getattr(pos, "quantity", 0) or 0) <= 0:
                        continue
                    targets.append((pid, ctx, code, pos))
        if not targets:
            return done
        today = now_shanghai().date()
        # ① 表值（一次查全量；同时登记 _peak_rows 供清仓清扫）
        table_peaks = self._load_position_peaks(db)
        with self._peak_lock:
            self._peak_rows = set(table_peaks.keys())
        period_cache: Dict[tuple, Optional[Decimal]] = {}
        snap_cache: Dict[tuple, Optional[Decimal]] = {}
        fallback_meta: List[tuple] = []
        recompute_meta: List[tuple] = []
        for pid, ctx, code, pos in targets:
            if (pid, code) in skip_keys:
                # 上一轮已补种（live_trades 重放路径）：不重复取数、不重复写表
                done.add((pid, code))
                continue
            done.add((pid, code))
            cur = getattr(pos, "highest_price", None) or Decimal("0")
            anchor, anchor_src = self._peak_anchor_date(
                db, pid, code, pos, first_buy_at, today)
            # 表值校验基准：快照 MAX 是纯本地查询，零 TQ
            skey = (code, anchor)
            if skey not in snap_cache:
                snap_cache[skey] = self._snapshot_peak_max(db, code, anchor)
            snap_peak = snap_cache[skey]
            table_val = table_peaks.get((pid, code))
            if table_val is not None and (
                    snap_peak is None or table_val[0] >= snap_peak):
                final = max(cur, table_val[0])
                if final > cur:
                    pos.update_high(final)
                # 表值已是持久化峰值，回写（幂等，不会写低）以刷新 updated_at
                self._note_peak_raised(pid, code, final, table_val[1])
                logger.info(
                    "[peak-seed:table] %s 周期=表值 表峰值=%s 快照MAX=%s 现值=%s "
                    "最终=%s （session %s, 组合 %s, 策略 %s）",
                    code, table_val[0], snap_peak, cur, final,
                    self.session_id, pid, ctx.strategy_id,
                )
                continue
            if table_val is not None:
                # 表值低于快照 MAX：历史退化值（如峰值被写成成本后又落表），
                # 不能当权威值用，走重算并取最大者自愈。
                logger.warning(
                    "[peak-seed:recompute] %s 表值 %s < 快照MAX %s，判为可疑，"
                    "重算并取最大者（session %s, 组合 %s, 策略 %s）",
                    code, table_val[0], snap_peak, self.session_id, pid,
                    ctx.strategy_id)
                recompute_meta.append(
                    (pid, ctx.strategy_id, code, table_val[0], snap_peak))
            strat_period = (getattr(ctx, "period", None) or "1m").lower()
            pkey = (code, anchor, strat_period)
            if pkey not in period_cache:
                period_cache[pkey] = self._period_close_peak(
                    code, strat_period, anchor, today)
            period_peak = period_cache[pkey]
            used_period = strat_period
            daily_peak = None
            if period_peak is None:
                if strat_period == "1d":
                    daily_peak = None  # 与 ② 同一次调用，已失败，不重复取
                else:
                    dkey = (code, anchor)
                    if dkey not in period_cache:
                        period_cache[dkey] = self._daily_close_peak(
                            code, anchor, today)
                    daily_peak = period_cache[dkey]
                    if daily_peak is not None:
                        used_period = "1d(降级)"
            final = cur
            for cand in (table_val[0] if table_val is not None else None,
                         period_peak, daily_peak, snap_peak):
                if cand is not None and cand > final:
                    final = cand
            if period_peak is not None:
                source = "period"
            elif daily_peak is not None:
                source = "daily"
            elif snap_peak is not None:
                source = "snapshot"
            else:
                source = "cost"
            if final > cur:
                pos.update_high(final)
            # 落库（只升不降；下一轮重启即可直接读表、不再取数）
            self._note_peak_raised(pid, code, final, None)
            logger.info(
                "[peak-seed:%s] %s 锚点=%s(%s) 使用周期=%s 周期峰值=%s 日线峰值=%s "
                "快照MAX=%s 最终=%s （session %s, 组合 %s, 策略 %s）",
                source, code, anchor.isoformat(), anchor_src, used_period,
                period_peak, daily_peak, snap_peak, final,
                self.session_id, pid, ctx.strategy_id,
            )
            if period_peak is None and daily_peak is None and snap_peak is None:
                fallback_meta.append((pid, ctx.strategy_id, code, anchor, cur))
        # 补种结果统一落库（不提交，recover 末尾由服务层提交）
        self._flush_position_peaks(db)
        for pid, strategy_id, code, table_pk, snap_pk in recompute_meta:
            self._recorder.record(
                gate="peak_seed",
                layer="live_gate",
                action="recompute",
                portfolio_id=pid,
                strategy_id=strategy_id,
                stock_code=code,
                bar_time=None,
                param_value=float(table_pk) if table_pk is not None else None,
                actual_value=float(snap_pk) if snap_pk is not None else None,
                message=("峰值表值可疑：表值 %s < 快照MAX %s，已重算并取最大者"
                         % (table_pk, snap_pk)),
            )
        if fallback_meta:
            for pid, strategy_id, code, anchor, cur in fallback_meta:
                logger.warning(
                    "[peak-seed:cost] %s 无周期/无日线/无快照峰值，沿用现有 %s（锚点 %s，session %s）",
                    code, cur, anchor.isoformat(), self.session_id)
                self._recorder.record(
                    gate="peak_seed",
                    layer="live_gate",
                    action="fallback",
                    portfolio_id=pid,
                    strategy_id=strategy_id,
                    stock_code=code,
                    bar_time=None,
                    message=("峰值补种无可用数据（策略周期/日线取数失败且无快照），"
                             "沿用现有峰值 %s，移动止损基准可能偏低（锚点 %s）"
                             % (cur, anchor.isoformat())),
                )
        if fallback_meta or recompute_meta:
            first_port = next(iter(ports_by_id.values()), None)
            if first_port is not None:
                self._drain_decisions(db, first_port)
        return done


    # ---------------- 持仓恢复 ----------------
    def recover(self, db: Session) -> None:
        """Core 重启后从 live_trades 重放，重建各组合虚拟持仓/虚拟现金。

        按 trade_time 顺序重放本 session 全部 live_trades：BUY 累加持仓+扣现金，
        SELL 减持仓+加现金。signal_type 从关联 LiveOrder 取（LiveTrade 无此列），
        用于 Position.apply_trade 区分 ADD（影响 add_count）。虚拟现金以成本计（§93）。
        """
        ports_by_id = {p.portfolio_id: p for p in self.portfolios}
        trades = (
            db.query(LiveTrade)
            .filter(LiveTrade.live_session_id == self.session_id)
            .order_by(LiveTrade.trade_time, LiveTrade.id)
            .all()
        )
        # 预取关联 LiveOrder 的 signal_type（LiveTrade 无 signal_type 列）
        order_ids = {t.live_order_id for t in trades if t.live_order_id is not None}
        sig_type_by_order: Dict[int, Optional[str]] = {}
        order_type_by_order: Dict[int, str] = {}
        if order_ids:
            for lo in db.query(LiveOrder).filter(LiveOrder.id.in_(order_ids)).all():
                sig_type_by_order[lo.id] = lo.signal_type
                order_type_by_order[lo.id] = lo.order_type or ""
        applied_by_order: Dict[int, tuple] = {}
        # 峰值补种锚点：每个 (组合, 代码) 最早一笔买入的成交时间（顺路取 min，零额外查询）
        first_buy_at: Dict[tuple, datetime] = {}
        for tr in trades:
            port = ports_by_id.get(tr.portfolio_strategy_id)
            if port is None:
                continue
            if (tr.trade_type or "").lower() == "buy":
                bkey = (tr.portfolio_strategy_id, tr.stock_code)
                prev_buy = first_buy_at.get(bkey)
                if (prev_buy is None
                        or (tr.trade_time is not None and tr.trade_time < prev_buy)):
                    first_buy_at[bkey] = tr.trade_time
            # 补记单（对账发现的陌生/人工成交，order_type=external）只留痕、不记账：
            # 会话运行期 record_external_deals 就刻意不 apply（持仓由桥快照镜面覆盖）。
            # 重启重放必须同口径——否则外部卖出会被 Position.sell 截断为 0 持仓却照加
            # 卖出所得 → 虚拟现金虚增（净值/单仓金额被放大，虽仍受真实可用资金兜底）。
            # 外部仓位重启后统一走「快照导入/镜面」作为继承仓按市值扣减，口径自洽。
            if order_type_by_order.get(tr.live_order_id) == "external":
                continue
            ctx = port.find_strategy(tr.strategy_id)
            if ctx is None:
                continue
            sig_type_str = sig_type_by_order.get(tr.live_order_id)
            trade = TradeEvent(
                strategy_id=tr.strategy_id,
                portfolio_id=tr.portfolio_strategy_id,
                stock_code=tr.stock_code,
                trade_type=TradeType(tr.trade_type.upper()),
                price=Decimal(str(tr.price)),
                quantity=tr.quantity,
                amount=Decimal(str(tr.amount)),
                commission=Decimal(str(tr.commission)),
                stamp_duty=Decimal(str(tr.stamp_duty)),
                trade_time=tr.trade_time,
                signal_type=SignalType(sig_type_str) if sig_type_str else None,
            )
            pos = ctx.positions.get(tr.stock_code)
            if pos is None:
                pos = Position(tr.stock_code)
                ctx.positions[tr.stock_code] = pos
            # 先持仓、后现金：持仓返回**实际扣减量**，现金按该量缩放后的成交额入账
            # （P0-1）。继承仓只有 SELL 没有 BUY 时虚拟持仓为 0 → 不入现金，
            # 保证「重启重放不改变虚拟现金」；人工减仓 + 在途卖单成交也只补一次。
            applied = pos.apply_trade(trade)
            if applied != trade.quantity:
                logger.warning(
                    "recover: SELL %s 成交 %s 超出虚拟持仓，按实际 %s 入账（P0-1，"
                    "portfolio %s, strategy %s, order %s）",
                    tr.stock_code, trade.quantity, applied,
                    tr.portfolio_strategy_id, tr.strategy_id, tr.live_order_id,
                )
                trade = trade.scaled_to(applied)
            if applied > 0:
                port.account.apply_trade(trade)
                # 风控冷静期恢复（与回测/回填共用 Portfolio.note_trade）：重启重放
                # 时从 live_trades 重建冷静期起点，避免重启即「冷却失效」。
                port.note_trade(trade)
            # 同步「已记账量」到委托状态机（重放后迟到成交/重复回填只补增量）
            if tr.live_order_id is not None:
                oq, oa, oc = applied_by_order.get(
                    tr.live_order_id, (0, Decimal("0"), Decimal("0")))
                applied_by_order[tr.live_order_id] = (
                    oq + int(tr.quantity or 0),
                    oa + Decimal(str(tr.amount or 0)),
                    oc + Decimal(str(tr.commission or 0)),
                )
        self._order_machine.seed_applied(applied_by_order)
        # 切片5 I4：挂回未完结 LiveOrder（submitted/partial）供主循环 _poll_deals 回填。
        # 跨重启场景：passorder 已发券商、Core 崩在落库前 → DB 无 live_trades 但有
        # submitted 记录，挂回后 _poll_deals 按 OrderRef 匹配真实成交补记（G2/G6）。
        pending = (
            db.query(LiveOrder)
            .filter(
                LiveOrder.live_session_id == self.session_id,
                LiveOrder.status.in_(["submitted", "partial"]),
            )
            .all()
        )
        # 在途集合只存「已加载的 detached 对象」：recover 后续还会 commit
        # （breaker.restore_counts / deferred 收尾等），不先摘出会 expired+detached。
        detach_loaded(db, *pending)
        self._pending_orders = {lo.id: lo for lo in pending}
        # A-4（C-2）：重启恢复单的 ref 并入 dispatcher 的 _known_refs（A-5 防重放基线），
        # 并按 created_at（UTC naive）换算上海时间补记 _submit_times——通达信委托表
        # 无时间列，重启后不补记则 legacy 窗口匹配（_ORDER_INSERT_TOLERANCE）不可用，
        # 挂回单的成交回填/认领会全部哑火。
        submit_times = getattr(self._dispatcher, "_submit_times", None)
        known_refs = getattr(self._dispatcher, "_known_refs", None)
        claimed_refs = getattr(self._dispatcher, "_claimed_refs", None)
        for lo in pending:
            ref = lo.order_ref
            if not ref:
                continue
            if known_refs is not None:
                known_refs.add(ref)
            # 批次 A 评审：挂回单的 ref 已绑定 DB 单（语义即已认领），必须同时进
            # _claimed_refs——否则重启后 _claimed_refs 为空，新单 fallback
            # 「形状匹配且未 claimed」可能绑到已恢复单的 ref，deals 按 ref 双份回填。
            if claimed_refs is not None:
                claimed_refs.add(ref)
            if submit_times is not None and ref not in submit_times:
                if lo.created_at is not None:
                    sh = lo.created_at + timedelta(hours=8)  # UTC naive → 上海 naive
                else:
                    sh = now_shanghai()
                submit_times[ref] = (sh.strftime("%Y%m%d"), sh.strftime("%H%M%S"))
        if pending:
            logger.info(
                "recover: %d pending live orders to backfill (session %s)",
                len(pending), self.session_id,
            )
        # ③ 外部现金流台账重放：虚拟现金随台账增减（入金加、出金减），同时记录
        # 累计净流入。放在成交重放之后（现金基数 = 起账资金 + 成交流水）、风控态
        # 还原之前（peak/prev_close 是剔除净流入后的口径，需先有净流入）。
        flows = []
        try:
            flows = (
                db.query(LiveCashFlow)
                .filter(LiveCashFlow.live_session_id == self.session_id)
                .all()
            )
        except Exception:  # noqa: BLE001 — 迁移未执行时无该表：降级为无台账，不阻断起账
            db.rollback()
            logger.warning(
                "recover: live_cash_flows 读取失败（是否未执行迁移？）→ 本次按无外部"
                "现金流重放 (session %s)", self.session_id, exc_info=True)
        net_by_portfolio: Dict[int, Decimal] = {}
        for flow in flows:
            try:
                amt = Decimal(str(flow.amount or 0))
            except (TypeError, ValueError):
                continue
            net_by_portfolio[flow.portfolio_strategy_id] = (
                net_by_portfolio.get(flow.portfolio_strategy_id, Decimal("0")) + amt)
        for pid, net in net_by_portfolio.items():
            port = ports_by_id.get(pid)
            if port is None or net == 0:
                continue
            port.account.cash += net
            port.external_net_inflow = net
            logger.info(
                "recover: 组合 %s 外部现金流累计净流入 %s → 虚拟现金 %s (session %s)",
                pid, fmt_money(net), fmt_money(port.account.cash), self.session_id)
        # P1-8：起账估值种子——从最近一次账户快照的 current_price 回填各组合的
        # 「最后已知价」（跨日持久）。否则重启后到首根 bar 之前，没有 bar 的停牌仓
        # 只能退回成本；成本未知（avg_cost=0）时会被按 0 估值 → 假回撤/假日亏。
        try:
            last_snap = (
                db.query(LiveAccountSnapshot)
                .filter(LiveAccountSnapshot.live_session_id == self.session_id)
                .order_by(LiveAccountSnapshot.id.desc())
                .first()
            )
            seeded: Dict[str, Decimal] = {}
            if last_snap is not None:
                for row in last_snap.positions:
                    try:
                        px = Decimal(str(row.current_price or 0))
                    except (TypeError, ValueError):
                        px = Decimal("0")
                    if px <= 0:
                        try:
                            qty = Decimal(str(row.quantity or 0))
                            if qty > 0:
                                px = Decimal(str(row.market_value or 0)) / qty
                        except (TypeError, ValueError):
                            px = Decimal("0")
                    if px > 0:
                        seeded[row.stock_code] = px
            for port in ports_by_id.values():
                for code, px in seeded.items():
                    port._price_snapshot.setdefault(code, px)
            if seeded:
                logger.info(
                    "recover: 估值价种子 %d 只（来自最近账户快照，session %s）",
                    len(seeded), self.session_id)
        except Exception:  # noqa: BLE001 — 种子失败不影响起账（退回成本/前收兜底）
            db.rollback()
            logger.warning(
                "recover: 从账户快照回填估值价失败（session %s）→ 估值走成本/前收兜底",
                self.session_id, exc_info=True)
        # D4：读回熔断计数（重启不丢累计次数，达 3 次转手动恢复）归 BreakerService
        # （0010 步骤 4）；预置 counts_written 避免首 bar 重复落库。
        self._breaker.restore_counts(db, ports_by_id)
        # 峰值补种（阶段二 A/B）：live_position_peaks 表值优先（有则不调 TQ）→ 该策略
        # 自身周期 close → 日线 → 快照 MAX → 成本，只升不降。必须在 deferred 队列重建
        # 之前（峰值只影响后续风控判定，不碰任何订单数据）。
        # 本轮只补 live_trades 重放出的持仓；继承仓（无成交记录）在下方导入后再补一次。
        seeded_keys = self._seed_position_peaks(db, ports_by_id, first_buy_at)
        # 峰值表清理：超过 30 个交易日未更新的行（已清仓但删行漏掉的残留）记 INFO。
        self._cleanup_stale_position_peaks(db)
        # 补种/清理结果立即提交：后续账户总览取不到时走不到末尾的 commit 分支，
        # 不提交就会导致「本次算了峰值、下次重启又重算」。峰值只影响风控判定，
        # 提前提交不影响其余恢复步骤的幂等性。
        try:
            db.commit()
        except Exception:  # noqa: BLE001 — 提交失败不阻断起账（下次重启重算）
            logger.warning("recover: commit peak seed failed (session %s)",
                           self.session_id, exc_info=True)
        # P2 持久化恢复：deferred 日线单重建内存队列，**DB 行保留**（2026-11-09
        # 起不再「转移删除」）——行 id 挂 order._deferred_row_id，放行提交成功时
        # 删旧行建 submitted 新行。这样 recover 之后到放行之前的任何时刻崩溃/
        # 停会话，都能再次从 DB 重建，不再有「内存有、库里无」的丢失窗口。
        # 重建失败的单记日志跳过（防坏行阻断其余恢复，行保留待人工处理）。
        deferred_rows = db.query(LiveOrder).filter(
            LiveOrder.live_session_id == self.session_id,
            LiveOrder.status == "deferred",
        ).all()
        today = now_shanghai().date()
        # 陈旧阈值 = 「上一交易日」而非日历天 -1（2026-11-09 修正）：旧实现用
        # today - 1 天，周五 defer → 周一启动会被误判陈旧（周五 < 周日）而丢弃。
        # 现在只丢「早于上一交易日」的残留：保住「当日 defer、次日（交易日）放行」。
        prev_trading_day = self._prev_trading_day(today)
        # 恢复行元快照（id/组合/策略/代码/方向/bar 日/数量）：commit 后 SessionLocal
        # （expire_on_commit=True）属性会过期，告警所需字段须在 commit 前取好。
        kept_meta: List[tuple] = []
        duplicate_meta: List[tuple] = []
        stale_meta: List[tuple] = []
        stale_dropped = 0
        duplicate_dropped = 0
        seen_keys: Set[tuple] = set()
        for lo in deferred_rows:
            # C 陈旧 deferred 丢弃（恢复期兜底）：bar_time 早于「上一交易日」的单不再
            # 执行——多为前日及更早残留，跨多日后放行等于拿过期信号追单。置 canceled
            # + 原因，不转入内存、不提交、不删行（保留审计）。
            if lo.bar_time is not None and lo.bar_time.date() < prev_trading_day:
                lo.status = "canceled"
                lo.error_message = (
                    "stale_restart: 陈旧 deferred 单（bar_time=%s，早于上一交易日 %s），"
                    "重启恢复时丢弃" % (lo.bar_time, prev_trading_day.isoformat())
                )
                stale_dropped += 1
                stale_meta.append((
                    lo.id, lo.portfolio_strategy_id, lo.strategy_id, lo.stock_code,
                    (lo.trade_type or "").lower(), lo.bar_time.date(), lo.quantity,
                ))
                logger.warning(
                    "recover: drop stale deferred id=%s %s %s bar_time=%s "
                    "(session %s; older than prev trading day %s)",
                    lo.id, lo.trade_type, lo.stock_code, lo.bar_time,
                    self.session_id, prev_trading_day,
                )
                continue
            # A 幂等键去重（恢复期）：旧版本/手工回灌可能留下同键多行（同组合+策略+
            # 股票+方向+数量+signal_bar_date）。保留首个，其余置 canceled，避免同一
            # 逻辑单被放行两次（D6 能挡第二次提交，但会留下永不清理的 deferred 行）。
            row_day = lo.bar_time.date() if lo.bar_time else None
            key = (lo.portfolio_strategy_id, lo.strategy_id, lo.stock_code,
                   (lo.trade_type or "").lower(), lo.quantity, row_day)
            if key in seen_keys:
                lo.status = "canceled"
                lo.error_message = (
                    "duplicate_deferred: 重启恢复发现同键重复 deferred 行"
                    "（保留首行，本行取消）"
                )
                duplicate_dropped += 1
                duplicate_meta.append((
                    lo.id, lo.portfolio_strategy_id, lo.strategy_id, lo.stock_code,
                    (lo.trade_type or "").lower(), row_day, lo.quantity,
                ))
                logger.warning(
                    "recover: drop duplicate deferred id=%s %s %s qty=%s bar=%s "
                    "(session %s)",
                    lo.id, lo.trade_type, lo.stock_code, lo.quantity,
                    lo.bar_time, self.session_id,
                )
                continue
            seen_keys.add(key)
            try:
                order = OrderEvent(
                    strategy_id=lo.strategy_id,
                    portfolio_id=lo.portfolio_strategy_id,
                    stock_code=lo.stock_code,
                    trade_type=TradeType(lo.trade_type.upper()),
                    signal_type=(SignalType(lo.signal_type)
                                 if lo.signal_type else SignalType.CLOSE),
                    quantity=lo.quantity,
                    price=lo.price,
                    bar_time=lo.bar_time,
                    signal_name=lo.signal_name or "",
                    # defer 的定义即 signal_bar_date=bar_time 当日（放行条件
                    # bar_time.date() < 当前 bar 日期由此成立）
                    signal_bar_date=lo.bar_time.date() if lo.bar_time else None,
                )
            except Exception:  # noqa: BLE001
                logger.exception(
                    "deferred row rebuild failed id=%s (session %s)",
                    lo.id, self.session_id)
                continue
            # 保留 DB 行（不再删）：行 id 挂单上，放行提交成功时由 _submit_one_order
            # 删旧行建 submitted 新行；再次重启仍能从该行重建。DB 是权威足迹。
            order._deferred_row_id = lo.id
            # P1：读回「已顺延一次」标记（首次顺延写 error_message，重启后靠它
            # 阻止第二次顺延；第二个到期日正常取消）。
            if "deferred_extended:" in str(lo.error_message or ""):
                order._deferred_extended = True
            # P1-3：读回重排次数（写在同一 error_message 标记里），重启后仍受上限约束。
            retry_count = _retry_count_from_message(lo.error_message)
            if retry_count:
                order._deferred_retry_count = retry_count
            self._signal_bar_deferred.append(order)
            kept_meta.append((
                lo.id, lo.portfolio_strategy_id, lo.strategy_id, lo.stock_code,
                (lo.trade_type or "").lower(), row_day, lo.quantity,
            ))
        if deferred_rows:
            db.commit()
            logger.info(
                "recover: %d deferred daily-signal order(s) restored, %d stale "
                "dropped, %d duplicate dropped (session %s; rows kept in DB)",
                len(kept_meta), stale_dropped, duplicate_dropped, self.session_id)
        # P0（2026-11-09）：陈旧 deferred 丢弃必须可见——旧实现只留 WARNING 日志，
        # 重启后单子静默消失（工作台无事件）。逐单发 risk 事件 +（可选）TQ 告警。
        for m in stale_meta:
            msg = (
                "重启恢复丢弃陈旧 deferred 单：%s %s 数量=%s bar_time=%s 早于上一"
                "交易日 %s（该日无行情或跨多日未放行），未下单"
                % ((m[4] or "").upper() or "-", m[3], m[6], m[5],
                   prev_trading_day.isoformat())
            )
            self._emit_risk_safe({
                "session_id": self.session_id,
                "rule": "deferred_dropped_stale",
                "row_id": m[0],
                "portfolio_id": m[1],
                "strategy_id": m[2],
                "stock_code": m[3],
                "trade_type": m[4],
                "quantity": m[6],
                "bar_date": m[5].isoformat() if m[5] else None,
                "prev_trading_day": prev_trading_day.isoformat(),
                "message": msg,
            })
            self._alert_message_safe("【实盘】%s" % msg)
        # D 恢复期异常告警（走既有 risk 事件流；只提示人工确认，不阻断启动）：
        # ① 重建条数超阈值：疑似日线轮被重复驱动 / 旧重复行未清理；
        # ② 同键多行（A 幂等键穿透，旧版本遗留）：同股同向同量同日多笔，可能是重复
        #    买卖单。分组键与 A 的去重键一致（含 strategy/quantity），避免把「同组合
        #    多策略各自止损」「同策略分批减仓」这类合法多笔误报为异常。
        if kept_meta:
            if len(kept_meta) > DEFERRED_RECOVER_ALERT_THRESHOLD:
                codes = sorted({m[3] for m in kept_meta})
                msg = (
                    "重启恢复发现 %d 条 deferred 日线单（阈值 %d），涉及 %s："
                    "疑似日线轮被重复驱动或旧重复行未清理，请人工确认后继续交易"
                    % (len(kept_meta), DEFERRED_RECOVER_ALERT_THRESHOLD, "、".join(codes))
                )
                logger.warning("recover: %s (session %s)", msg, self.session_id)
                self._emit("risk", {
                    "session_id": self.session_id,
                    "rule": "deferred_recover_volume",
                    "count": len(kept_meta),
                    "threshold": DEFERRED_RECOVER_ALERT_THRESHOLD,
                    "stock_codes": codes,
                    "message": msg,
                })
            groups: Dict[tuple, List[tuple]] = {}
            for m in list(kept_meta) + list(duplicate_meta):
                # 键 = (组合, 策略, 代码, 方向, bar 日, 数量)
                groups.setdefault((m[1], m[2], m[3], m[4], m[5], m[6]), []).append(m)
            for key, metas in groups.items():
                if len(metas) < 2:
                    continue
                row_ids = [m[0] for m in metas]
                kept_id = min(row_ids)   # 去重保留最小 row id，其余已置 canceled
                msg = (
                    "重启恢复发现同键 deferred 多行（%s %s %s 数量=%s）：row id=%s，"
                    "已自动取消重复行（保留 row id=%s），疑似重复买卖单，需人工确认"
                    % (key[2], (key[3] or "").upper(), key[4], key[5], row_ids, kept_id)
                )
                logger.warning("recover: %s (session %s)", msg, self.session_id)
                self._emit("risk", {
                    "session_id": self.session_id,
                    "rule": "deferred_recover_duplicate",
                    "stock_code": key[2],
                    "trade_type": (key[3] or "").upper(),
                    "bar_date": key[4].isoformat() if key[4] else None,
                    "quantity": key[5],
                    "count": len(metas),
                    "row_ids": row_ids,
                    "message": msg,
                })
        # 日切幂等跨重启：当日日线信号已落库 → 恢复 DailyCloser 内存标记，
        # 防启动重驱动当日日线轮重复 defer/trigger（2026-11-03 实证 57 条重复）。
        # B（advance_close_enabled=1）时 14:50 提前轮次也会写当日 live_signals，
        # 这里不能据此判定「15:05 收盘落定轮次已跑」——否则周五/月末重启会跳过
        # 当日 1w/1mon 周期末轮次。B 开启时交给 daily_bars_done_date 判定。
        _today = now_shanghai().date()
        _has_today = db.query(LiveSignal.id).filter(
            LiveSignal.live_session_id == self.session_id,
            LiveSignal.bar_time == datetime.combine(_today, datetime.min.time()),
        ).first()
        if _has_today and not getattr(self, "_advance_close_enabled_cfg", False):
            self._closer.last_daily_bar_date = _today
            logger.info(
                "recover: today's daily bar already driven, skip re-drive "
                "(session %s)", self.session_id)
        # 启动期顺序：先导入外部持仓，再对账。recover 的虚拟账本只来自 live_trades
        # 重放，柜台原有持仓（有仓无账）此刻尚未导入；直接对账会产出
        # 「virtual=0 / real=5300」假差异（重启瞬间旧照片 + 假告警）。
        # 读取失败/导入异常不吞问题：跳过导入，仍按原逻辑对账，真实差异照旧保留。
        try:
            overview = self._read_account_overview()
            if overview is not None:
                _, positions = overview
                self._import_external_positions(db, positions, now_shanghai())
                # 继承仓（有仓无账）在上一轮补种时还不在 ctx.positions 里，三级重算
                # 全被跳过，峰值只能靠导入路径的 prev_hp/成本兜底。导入后补一次，
                # skip_keys 排除已补过的票（不重复取数、不重复写表）。
                self._seed_position_peaks(
                    db, ports_by_id, first_buy_at, skip_keys=seeded_keys)
                # 导入会顺带落「池外仓汇总」决策事件（观测）；recover 末尾无统一
                # commit，这里显式提交，避免事件随 Session 关闭被回滚。
                db.commit()
        except Exception:  # noqa: BLE001
            logger.exception(
                "recover: startup external position import failed (session %s)",
                self.session_id)
        # D3：虚拟持仓 vs 桥实际 /positions 对账（仅告警不修正，见 _reconcile_positions）
        self._reconcile_positions()

    def _reconcile_positions(self) -> None:
        """D3：对账——虚拟持仓(按 code 聚合) vs 桥实际 /positions，不一致仅记录+告警。

        recover 重建的虚拟持仓以 live_trades 为唯一源；桥 /positions 是账户真实持仓。
        比对口径：按 stock_code 聚合所有组合策略虚拟净持仓 vs 桥 volume。
        差异记 `_reconcile_mismatches` + 告警日志 + risk SSE 事件（I-7：工作台事件
        日志立即可见，无需翻 Core 日志），**不自动修正账面**（首期安全：
        在途单未回填/桥行情延迟期可能假不一致，自动修正反而引入错账；真机跑顺后
        如需自动校准再放开）。桥离线 → 记日志跳过（保留上次差异结果，不闪空）。
        附加提示：实际有仓但虚拟无仓（如 Core 宕机期手动下单）也记录，供人工核对。
        """
        try:
            rows = self._dispatcher.query_positions()
        except BridgeUnavailableError:
            logger.warning("reconcile skipped: bridge offline (session %s)", self.session_id)
            return
        if rows is None:
            # 读失败 ≠ 真空仓：保留上次对账结果，不能把 real 当 0 算差异。
            logger.warning(
                "reconcile skipped: positions unavailable (session %s): %s",
                self.session_id,
                getattr(self._dispatcher, "last_error", "") or "unknown")
            return
        real: Dict[str, int] = {}
        for r in rows:
            inst = r.get("instrument")
            exch = r.get("exchange")
            vol = r.get("volume")
            if inst and exch and vol is not None:
                real["%s.%s" % (inst, exch)] = int(vol)
        virtual: Dict[str, int] = {}
        for port in self.portfolios:
            for ctx in port.strategies:
                for code, pos in ctx.positions.items():
                    if pos.quantity == 0:
                        continue
                    virtual[code] = virtual.get(code, 0) + pos.quantity
        diffs = []
        for code in sorted(set(real) | set(virtual)):
            v = virtual.get(code, 0)
            r = real.get(code, 0)
            if v == r:
                continue
            diffs.append((code, v, r))
            # I-7（audit）：差异推 risk 事件（形状照既有 risk：rule + message）
            self._emit("risk", {
                "rule": "reconcile_mismatch",
                "stock_code": code,
                "virtual": v,
                "real": r,
                "diff": v - r,
                "message": "持仓对账差异 %s：虚拟=%d 桥实际=%d 差=%d" % (code, v, r, v - r),
            })
            logger.warning(
                "reconcile mismatch (session %s) %s: virtual=%d real=%d diff=%d",
                self.session_id, code, v, r, v - r,
            )
        self._set_reconcile_mismatches(diffs)

    def _set_reconcile_mismatches(
        self, diffs, computed_at: Optional[datetime] = None
    ) -> None:
        """整体替换对账面（空集也清空——旧照片不能留到下一次对账）。

        只在成功算得差异时调用；读取/计算失败路径不调用 → 保留旧值与旧时间戳。
        computed_at 为本次差异的计算时点，每行携带，前端据此显示「算于何时」。
        """
        stamp = (computed_at or now_shanghai()).isoformat()
        self._reconcile_mismatches = [
            {"code": code, "virtual": v, "real": r, "diff": v - r,
             "computed_at": stamp}
            for code, v, r in diffs
        ]

    def _apply_close_counter_correction(self, now: Optional[datetime] = None) -> int:
        """收盘对账：按柜台口径修正虚拟台账（每交易日一次，幂等）+ 保留审计行。

        只处理「虚拟有量、柜台整行缺失」的代码（人工/外部清仓）：清零虚拟持仓 +
        按账面成本退回虚拟现金 + 落 ``manual_intervention`` 审计
        （action=apply / reason=close_reconcile_counter_zero，含代码、虚拟量、
        真实量、差额、方向），可回溯「哪一天因为什么被修正过」。

        不处理柜台有量而虚拟缺失/偏少的方向：池外仓由 ``out_of_pool_position``
        口径隔离，池内仓由每轮采集的导入/镜面覆盖处理；有在途单的代码一律跳过
        （成交可能仍在路上，交给 expire/backfill 收口）。
        """
        targets = [
            m for m in self._reconcile_mismatches
            if int(m.get("virtual") or 0) > 0 and not int(m.get("real") or 0)
        ]
        if not targets:
            return 0
        query_date = (now or now_shanghai()).date()
        db = self._db_session_factory()
        cleared: list = []
        try:
            inflight = {
                row[0] for row in db.query(LiveOrder.stock_code).filter(
                    LiveOrder.live_session_id == self.session_id,
                    LiveOrder.status.in_(["submitted", "partial"]),
                ).all()
            }
            for m in targets:
                code = m["code"]
                if code in inflight:
                    continue
                for port in self.ctx.portfolios:
                    for ctx in (getattr(port, "strategies", None) or []):
                        pos = (getattr(ctx, "positions", None) or {}).get(code)
                        if pos is None or pos.quantity <= 0:
                            continue
                        qty = pos.quantity
                        released = (pos.avg_cost or Decimal("0")) * Decimal(int(qty))
                        with self._book_lock:
                            self._credit_released_value(
                                port, released, "close_reconcile")
                            pos.mirror_bridge(0, 0, None, query_date)
                        self._note_manual_intervention(
                            db, code, qty, 0, direction="decrease",
                            reason="close_reconcile_counter_zero", action="apply",
                            source="close_reconcile")
                        cleared.append(code)
            db.commit()
        except Exception:  # noqa: BLE001 — 修正失败不阻断收盘流程，只留日志
            db.rollback()
            logger.exception(
                "close counter correction failed (session %s)", self.session_id)
            return 0
        finally:
            db.close()
        if cleared:
            done = set(cleared)
            self._reconcile_mismatches = [
                m for m in self._reconcile_mismatches if m["code"] not in done
            ]
            logger.warning(
                "close reconcile: virtual ledger corrected to counter for %s "
                "(session %s)", ", ".join(sorted(done)), self.session_id)
        return len(cleared)

    def _maybe_reconcile_after_close(self, now: Optional[datetime] = None) -> None:
        """I-7（audit）：每日收盘清扫完成后复查一次对账（幂等，每交易日一次）。

        挂点在 _tick_main 的 _maybe_close_sweep 之后：此时在途单已终态收敛、
        成交已回填，对账差异最可信。用 closer 的 last_close_sweep_date 判定
        「清扫已完成」（deferred 重试轮不置位 → 顺延到清扫真正完成的那轮）。
        """
        today = (now or now_shanghai()).date()
        if self._last_reconcile_date == today:
            return
        if self._last_close_sweep_date != today:
            return
        self._last_reconcile_date = today
        self._reconcile_positions()
        # 人工干预收口（2026-11-11）：收盘后把「虚拟有、柜台整行缺失」的仓按柜台
        # 口径清零并留审计；只修正这一方向，不做反向写入或对池外仓动手。
        self._apply_close_counter_correction(now)

    @property
    def reconcile_mismatches(self) -> List[dict]:
        """I-7：最近一次对账差异摘要（[] = 无差异/未对账），session API 序列化用。"""
        return self._reconcile_mismatches

    @property
    def bridge_online(self) -> bool:
        """桥是否在线（心跳/下单成功为 True，离线暂停期间为 False）。"""
        return self._bridge_online

    @property
    def pending_orders_count(self) -> int:
        """在途未完结单数（submitted/partial），供 session API 桥状态并入（G7）。"""
        return len(self._pending_orders)

    @property
    def last_backfill_time(self) -> Optional[datetime]:
        """最近一次 /deals 成交回报回填时点（None=尚无回填）。"""
        return self._last_backfill_time

    @property
    def dispatcher(self):
        """暴露 dispatcher 供桥状态查询等只读访问。"""
        return self._dispatcher
