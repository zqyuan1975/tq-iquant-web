"""公司行为 / 停牌 / 退市判定内核（回测与实盘共用，2026-11-18）。

**唯一一份判定逻辑**：回测 ``BacktestEngine`` 与实盘 ``LiveEngine`` 都调用
``apply_day``，两端只负责「取事件」（各自从 DB 读同一张 ``corporate_actions``）
与「落账」（回测改 ``Position``/``Account``，实盘分红改虚拟现金、送转交给柜台
镜面）。内核不碰 DB、不碰 dispatcher、不写事件，只做三件事：

1. 计算当日应当生效的事件（含「事件日非交易日顺延到下一交易日、只生效一次」）；
2. 把钱/股数变动落到 ``Position`` / ``Account``（送转可关，实盘交给镜面）；
3. 返回 ``AppliedAction`` 意图列表，由适配层决定再投资成交、退市强平、停牌闸、
   告警等副作用。

**默认路径 = 空**：``timeline is None`` 或 ``is_empty()`` 时立即返回，不改任何
对象、不产生任何事件 —— 这是「未启用行为逐字一致」的结构性保证。
"""
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from .market_rules import trading_days_between

# 口径版本：改动 apply_day 的判定/落账语义时必须同步升级，并写进
# params_snapshot["corporate_actions"]["version"] 与增量 schema 指纹。
CORPORATE_ACTION_VERSION = "manual_events_v1"
# 退市/停牌处置成交价基准：最后可交易价（无价则退回成本）。
DELIST_PRICE_BASIS = "last_tradeable_close_v1"

EVENT_TYPES = ("dividend", "split", "rights", "delist", "suspend")

# AppliedAction.kind 枚举（适配层据此决定副作用）
KIND_DIVIDEND_CASH = "dividend_cash"
KIND_SKIP_CASH = "skip_cash"
KIND_REINVEST = "reinvest"
KIND_SPLIT = "split"
KIND_DELIST_CLOSE = "delist_close"
KIND_SUSPEND = "suspend_block"
KIND_NOTICE = "notice"

_D = Decimal("0")


@dataclass(frozen=True)
class CorporateActionEvent:
    code: str
    event_type: str
    ex_date: date
    end_date: Optional[date] = None
    cash_per_share: Optional[Decimal] = None
    cash_pct: Optional[Decimal] = None
    share_ratio: Optional[Decimal] = None
    rights_price: Optional[Decimal] = None
    note: Optional[str] = None


@dataclass
class AppliedAction:
    """内核发出的「意图」，由适配层落账。"""
    kind: str
    code: str
    cash: Decimal = _D
    qty: int = 0
    price: Optional[Decimal] = None
    strategy_id: Optional[int] = None
    reason: str = ""


class CorporateActionTimeline:
    """按 (code, ex_date) 索引的事件集合，纯内存、可跨回测/实盘复用。"""

    def __init__(self, events: Iterable[CorporateActionEvent]):
        self._events: List[CorporateActionEvent] = sorted(
            events, key=lambda e: (e.ex_date, e.code, e.event_type))
        self._by_code: Dict[str, List[CorporateActionEvent]] = {}
        for ev in self._events:
            self._by_code.setdefault(ev.code, []).append(ev)

    def __len__(self) -> int:
        return len(self._events)

    def is_empty(self) -> bool:
        return not self._events

    def events_up_to(self, day: date,
                     after: Optional[date] = None) -> List[CorporateActionEvent]:
        """返回 ``ex_date <= day`` 且 ``ex_date > after`` 的事件（天然处理顺延）。"""
        return [e for e in self._events
                if e.ex_date <= day and (after is None or e.ex_date > after)]

    def is_suspended(self, code: str, day: date) -> bool:
        """该代码当日是否处于停牌区间（``ex_date <= day <= end_date``，end 含当日）。"""
        for e in self._by_code.get(code, ()):
            if e.event_type != "suspend":
                continue
            if e.ex_date <= day and (e.end_date is None or day <= e.end_date):
                return True
        return False

    def suspended_codes(self, day: date) -> frozenset:
        return frozenset(c for c in self._by_code if self.is_suspended(c, day))

    def ex_rights_days(self) -> Dict[Tuple[str, date], Tuple[Decimal, Decimal]]:
        """按 ``(code, ex_date)`` 汇总除权除息参考价所需的「每股现金 + 送转比例」。

        交易所除权参考价 = (前收 - 每股现金红利) / (1 + 每股送转比例)。配股不参与
        （与 ``apply_day`` 放弃配股同口径）。同一天既有分红又有送转时按加总处理。
        """
        out: Dict[Tuple[str, date], list] = {}
        for ev in self._events:
            if ev.event_type not in ("dividend", "split"):
                continue
            cell = out.setdefault((ev.code, ev.ex_date), [Decimal("0"), Decimal("0")])
            if ev.event_type == "dividend" and ev.cash_per_share is not None:
                cell[0] += ev.cash_per_share
            elif ev.event_type == "split" and ev.share_ratio is not None:
                cell[1] += ev.share_ratio
        return {k: (v[0], v[1]) for k, v in out.items() if v[0] or v[1]}

    def digest(self) -> str:
        """事件集合指纹（进 params_snapshot，供跨记录口径核对）。"""
        import hashlib
        import json

        rows = [[e.code, e.event_type, e.ex_date.isoformat(),
                 e.end_date.isoformat() if e.end_date else None,
                 str(e.cash_per_share) if e.cash_per_share is not None else None,
                 str(e.cash_pct) if e.cash_pct is not None else None,
                 str(e.share_ratio) if e.share_ratio is not None else None,
                 str(e.rights_price) if e.rights_price is not None else None]
                for e in self._events]
        return hashlib.sha1(
            json.dumps(rows, separators=(",", ":")).encode("utf-8")).hexdigest()


def _iter_holding(portfolio, code: str):
    """遍历该代码所有非空持仓（一个组合下可被多个策略同时持有）。"""
    for ctx in getattr(portfolio, "strategies", ()) or ():
        pos = ctx.positions.get(code)
        if pos is not None and pos.quantity > 0:
            yield ctx, pos


def _apply_split(pos, share_ratio, *, code: str = "",
                 ex_date: Optional[date] = None,
                 next_trade_day: Optional[Callable[[date], Optional[date]]] = None) -> int:
    """送转：股数增加、总成本不变（均价按比例摊薄）、最高价按比例下调。

    T+1 桶口径（P1-3，2026-11-23，按交易所规则）：

    - **沪市 / 北交所**（``.SH`` / ``.BJ``）：送转/转增所得**新增股份**在除权日
      不可卖、次一交易日可卖——即除权日不可卖出的增量，记为 ``ex_date`` 桶
      （本系统桶键为取得日、可卖判据 ``d < query_date``，故等价于次日起可卖）；
      原持仓各桶数量与日期保持不变（原股除权日照旧可卖）。取不到下一交易日时
      回落到原「等比放大」口径。
    - **深市**（``.SZ`` 及其它）：维持原口径——各桶等比放大、日期不变。

    配股（``rights``）当前仍整体放弃（``apply_day`` 只记事件、不缴款不改账），
    因此本函数只处理送转/转增；若未来启用配股，应复用同一落桶规则。

    返回新增股数（0 = 无变动）。整数取整误差记在最后一桶，保证
    ``sum(_lots) == quantity``。
    """
    if share_ratio is None or share_ratio <= 0 or pos.quantity <= 0:
        return 0
    old_qty = int(pos.quantity)
    new_qty = int(Decimal(old_qty) * (Decimal("1") + share_ratio))
    if new_qty <= old_qty:
        return 0
    ratio = Decimal(new_qty) / Decimal(old_qty)
    total_cost = pos.avg_cost * Decimal(old_qty)
    pos.quantity = new_qty
    pos.avg_cost = total_cost / Decimal(new_qty)
    if pos.highest_price and pos.highest_price > 0:
        pos.highest_price = pos.highest_price / ratio
    pos.strategy_quantity = int(Decimal(pos.strategy_quantity) * ratio)
    pos.platform_quantity = min(
        new_qty, int(Decimal(pos.platform_quantity) * ratio))
    if not pos._lots:
        return new_qty - old_qty

    is_sh_or_bj = str(code).upper().endswith((".SH", ".BJ"))
    next_day = None
    if is_sh_or_bj and ex_date is not None and next_trade_day is not None:
        try:
            next_day = next_trade_day(ex_date)
        except Exception:  # noqa: BLE001 — 日历异常回落原口径
            next_day = None
    if next_day is not None:
        # 原桶总量校准为 old_qty（异常数据时差额扣/补最后一桶），新增股单独落桶。
        # 注意：本系统 _lots 的键是「取得日」，可卖判据是 d < query_date，因此
        # 新增股要「除权日不可卖、次一交易日可卖」必须记为 ex_date 桶（等价于
        # 用户口径的「ex_date+1 起可卖」；若记成 next_day 会再晚一天）。
        lots = {d: q for d, q in pos._lots.items() if q > 0}
        diff = old_qty - sum(lots.values())
        if lots:
            last = max(lots)
            lots[last] += diff
            if lots[last] <= 0:
                lots.pop(last, None)
        delta = new_qty - old_qty
        if delta > 0:
            lots[ex_date] = lots.get(ex_date, 0) + delta
        pos._lots = lots
        return new_qty - old_qty

    # 深市 / 无下一交易日：原「等比放大」口径，取整误差补到最后一桶。
    scaled = {d: int(Decimal(q) * ratio) for d, q in pos._lots.items()}
    diff = new_qty - sum(scaled.values())
    last = max(scaled)
    scaled[last] += diff
    pos._lots = {d: q for d, q in scaled.items() if q > 0}
    return new_qty - old_qty


def _dividend_amount(event: CorporateActionEvent, price: Optional[Decimal],
                     quantity: int, mode: str) -> Optional[Decimal]:
    """分红金额。

    - ``mode="backtest"``：复权价空间只能用比例口径 ``cash_pct``（前复权是加性
      压缩，每股金额不可还原），金额 = cash_pct × 参照前收 × 持股数；
    - ``mode="backtest_raw"``：不复权原始价空间（J4 exact 档），优先按每股税后
      金额 ``cash_per_share`` 入账，缺失时回落比例口径；
    - ``mode="live"``：柜台口径只能给每股税后金额 ``cash_per_share``；
    - 对应字段缺失 → None（适配层记 skip_cash 事件，绝不猜）。
    """
    if quantity <= 0:
        return None
    if mode == "live":
        if event.cash_per_share is None:
            return None
        return (event.cash_per_share * Decimal(quantity)).quantize(Decimal("0.01"))
    if mode == "backtest_raw" and event.cash_per_share is not None:
        return (event.cash_per_share * Decimal(quantity)).quantize(Decimal("0.01"))
    if event.cash_pct is None:
        return None
    if price is None or price <= 0:
        return None
    return (event.cash_pct * price * Decimal(quantity)).quantize(Decimal("0.01"))


def apply_day(portfolio, timeline: Optional[CorporateActionTimeline], day: date,
              recorder=None, *, mode: str = "backtest",
              reinvest_enabled: bool = False,
              price_resolver: Optional[Callable[[str], Optional[Decimal]]] = None,
              dispose_days: int = 0,
              delist_force_close: bool = False,
              apply_split: bool = True,
              last_action_date: Optional[date] = None,
              next_trade_day: Optional[Callable[[date], Optional[date]]] = None) -> List[AppliedAction]:
    """把当日公司行为落到 ``portfolio``，返回意图列表。

    参数：
    - ``mode``：``backtest`` / ``backtest_raw`` / ``live``（决定分红用比例还是
      每股金额口径）。
    - ``apply_split``：False = 不手工改持仓数量（实盘交给柜台镜面），只发 split 意图。
    - ``dispose_days``：连续停牌超过 N 个交易日 → 发一次 delist_close 意图；0 = 不处置。
    - ``delist_force_close``：退市事件是否强平（False = 只发 notice）。
    - ``last_action_date``：上次生效日（调用方维护），保证事件只生效一次 + 顺延。
    - ``next_trade_day``：给定日期返回下一交易日（P1-3）。仅沪市/北交所送转所得
      新增股用它落 ``ex_date + 1`` 桶；缺省时回落实盘原有「等比放大」口径。
    """
    if timeline is None or timeline.is_empty():
        return []

    actions: List[AppliedAction] = []
    events = timeline.events_up_to(day, after=last_action_date)
    by_code: Dict[str, List[CorporateActionEvent]] = {}
    for ev in events:
        by_code.setdefault(ev.code, []).append(ev)

    # 顺序固定：送转（先改股数）→ 分红（按新股数算每股）→ 再投资 → 退市 → 停牌。
    for code in sorted(by_code):
        evs = by_code[code]
        holdings = list(_iter_holding(portfolio, code))

        for ev in evs:
            if ev.event_type != "split":
                continue
            for ctx, pos in holdings:
                if apply_split:
                    delta = _apply_split(
                        pos, ev.share_ratio, code=code, ex_date=ev.ex_date,
                        next_trade_day=next_trade_day)
                    if delta:
                        actions.append(AppliedAction(
                            KIND_SPLIT, code, qty=delta,
                            strategy_id=getattr(ctx, "strategy_id", None),
                            reason="share_ratio=%s" % ev.share_ratio))
                else:
                    actions.append(AppliedAction(
                        KIND_SPLIT, code, qty=0,
                        strategy_id=getattr(ctx, "strategy_id", None),
                        reason="mirror"))

        for ev in evs:
            if ev.event_type != "dividend":
                continue
            for ctx, pos in holdings:
                price = price_resolver(code) if price_resolver else None
                amount = _dividend_amount(ev, price, pos.quantity, mode)
                if amount is None:
                    actions.append(AppliedAction(
                        KIND_SKIP_CASH, code,
                        strategy_id=getattr(ctx, "strategy_id", None),
                        reason="cash field missing for mode=%s" % mode))
                    continue
                if amount <= 0:
                    continue
                # 分红现金入组合账户（实盘与回测同一处落账；实盘另有台账持久化）
                portfolio.account.add_cash(amount)
                # 除息基准调整（2026-11-25）：不复权价空间下除息日价格下跳≈每股
                # 派息，若成本/峰值不随之同减，"分红"会被算成"亏损/回撤"而凭空
                # 触发固定止损/移动止损。前复权/后复权序列在除息日本就连续，
                # 此时再减一次属双重扣减，必须跳过。
                if mode in ("backtest_raw", "live"):
                    dps = amount / Decimal(pos.quantity)
                    pos.adjust_cash_dividend(
                        dps, adjust_cost=(mode != "live"))
                    hook = getattr(portfolio, "peak_adjust_hook", None)
                    if hook is not None and pos.highest_price > 0:
                        try:
                            hook(portfolio.portfolio_id, code,
                                 pos.highest_price)
                        except Exception:  # noqa: BLE001 — 观测链路不得打断主链路
                            pass
                actions.append(AppliedAction(
                    KIND_DIVIDEND_CASH, code, cash=amount,
                    strategy_id=getattr(ctx, "strategy_id", None),
                    reason="cash_per_share=%s cash_pct=%s" % (
                        ev.cash_per_share, ev.cash_pct)))
                if reinvest_enabled:
                    actions.append(AppliedAction(
                        KIND_REINVEST, code, cash=amount,
                        strategy_id=getattr(ctx, "strategy_id", None),
                        reason="reinvest"))

        for ev in evs:
            if ev.event_type != "rights":
                continue
            # 配股默认放弃：只记事件，不缴款、不改账、不下单（本批范围外）
            actions.append(AppliedAction(
                KIND_NOTICE, code,
                reason="rights_unhandled: waived ratio=%s price=%s" % (
                    ev.share_ratio, ev.rights_price)))

        for ev in evs:
            if ev.event_type != "delist":
                continue
            for ctx, pos in holdings:
                price = price_resolver(code) if price_resolver else None
                actions.append(AppliedAction(
                    KIND_DELIST_CLOSE if delist_force_close else KIND_NOTICE,
                    code, qty=pos.quantity, price=price,
                    strategy_id=getattr(ctx, "strategy_id", None),
                    reason="delist last_trade_date=%s basis=%s" % (
                        ev.ex_date, DELIST_PRICE_BASIS)))

    # 停牌：当日仍处于停牌区间的代码 → 挂到组合上（OPEN/ADD 闸读它）。
    prev_suspended = getattr(portfolio, "suspended_codes", None) or frozenset()
    suspended = timeline.suspended_codes(day)
    portfolio.suspended_codes = suspended
    for code in sorted(set(suspended) - set(prev_suspended)):
        if list(_iter_holding(portfolio, code)):
            actions.append(AppliedAction(
                KIND_SUSPEND, code, reason="suspended on %s" % day))

    # 长期停牌处置（默认 0 = 不处置）：超过 N 个交易日只发一次强平意图。
    if dispose_days and dispose_days > 0:
        disposed = getattr(portfolio, "_ca_disposed", None)
        if disposed is None:
            disposed = set()
            portfolio._ca_disposed = disposed
        for code in sorted(suspended):
            if not list(_iter_holding(portfolio, code)):
                continue
            ev = _active_suspend_event(timeline, code, day)
            if ev is None:
                continue
            key = (code, ev.ex_date)
            if key in disposed:
                continue
            if trading_days_between(ev.ex_date, day) + 1 < dispose_days:
                continue
            disposed.add(key)
            for ctx, pos in _iter_holding(portfolio, code):
                price = price_resolver(code) if price_resolver else None
                actions.append(AppliedAction(
                    KIND_DELIST_CLOSE if delist_force_close else KIND_NOTICE,
                    code, qty=pos.quantity, price=price,
                    strategy_id=getattr(ctx, "strategy_id", None),
                    reason="suspend_dispose days=%d since=%s" % (
                        dispose_days, ev.ex_date)))

    return actions


def _active_suspend_event(timeline: CorporateActionTimeline, code: str,
                          day: date) -> Optional[CorporateActionEvent]:
    for e in timeline._by_code.get(code, ()):
        if e.event_type != "suspend":
            continue
        if e.ex_date <= day and (e.end_date is None or day <= e.end_date):
            return e
    return None


def is_suspended(timeline: Optional[CorporateActionTimeline], code: str,
                 day: date) -> bool:
    """单点查询（供无需 apply_day 的调用方使用）；无 timeline 恒 False。"""
    if timeline is None:
        return False
    return timeline.is_suspended(code, day)


def build_timeline_from_rows(rows) -> CorporateActionTimeline:
    """ORM 行 / 标量 dict → 事件集（避免内核依赖 SQLAlchemy 模型类）。"""
    events: List[CorporateActionEvent] = []
    for row in rows or ():
        if isinstance(row, dict):
            get = row.get
        else:
            def get(key, _row=row):
                return getattr(_row, key, None)
        ex_date = get("ex_date")
        if ex_date is None:
            continue
        events.append(CorporateActionEvent(
            code=str(get("code") or ""),
            event_type=str(get("event_type") or ""),
            ex_date=ex_date,
            end_date=get("end_date"),
            cash_per_share=_dec(get("cash_per_share")),
            cash_pct=_dec(get("cash_pct")),
            share_ratio=_dec(get("share_ratio")),
            rights_price=_dec(get("rights_price")),
            note=get("note"),
        ))
    return CorporateActionTimeline(events)


def _dec(value) -> Optional[Decimal]:
    if value is None:
        return None
    return value if isinstance(value, Decimal) else Decimal(str(value))
