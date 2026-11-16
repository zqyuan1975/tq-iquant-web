from dataclasses import dataclass, field, replace
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, Optional

from tq_iquant_shared.constants import SignalType, TradeType


@dataclass
class BarEvent:
    stocks: Dict[str, Dict[str, object]] = field(default_factory=dict)
    bar_time: Optional[datetime] = None
    # C6：驱动 bar 所属周期（"1m"/"5m"/"1d"...）。None=未标注（回测/旧调用，处理全部策略）。
    period: Optional[str] = None
    # BarPoller 本轮该 bar 各 code 拉到的原始 bar dict 列表（count 窗口，含 forming 最新一根），
    # 透传给注入复用——消除 1m 双拉：BarPoller 已拉判完成，注入并入预热缓存直接复用不再重拉。
    # None=非 BarPoller 路径（回测/手动构造），注入走 _get_bars_with_increment 增量拉。
    bars_by_code: Optional[Dict[str, list]] = None
    # 14:51 提前收盘价轮次标记：True = 该 1d bar 由 advance_close 轮次驱动，
    # 其信号当日提交（尾盘成交），不再受 risk_timing=next_day 的次日延迟约束。
    # 回测/收盘落定后的日线轮次（1d/1w/1mon）/14:30 日终均为 False，行为不变。
    advance_close: bool = False
    # 收盘落定轮次标记：True = 该日线级别 bar 由「周期收盘价落定后」（15:05）轮次驱动。
    # 此时即便是 risk_timing=immediate 的策略也必须 defer 到次一交易日首 bar 提交——
    # 收盘后报单报不进交易所，只会成为待报/废单，与回测「次日开盘成交」也不符。
    post_close: bool = False
    # 委托价覆盖（仅 B「14:50 提前收盘价」轮次）：code → 执行价（14:52 起始 1m bar 的
    # 开盘价，14:51 兜底）。只影响 OrderEvent.price（委托价）；数量/资金/仓位计算仍用
    # stocks[code]["close"]；持仓成本由成交回填/券商快照决定，与本字段无关。
    exec_prices: Optional[Dict[str, float]] = None
    # 基线 bar 标记：True = BarPoller 首次建立基线/离线恢复时交付的那根完成 bar。
    # 语义：不跑公式信号评估（防重放历史 bar），但允许放行已决定好的 deferred 单
    # （首根完成 bar 不必被基线白白吞掉）。默认 False = 正常触发 bar。
    baseline: bool = False


@dataclass
class SignalEvent:
    strategy_id: int
    stock_code: str
    signal_name: str
    signal_type: SignalType
    bar_time: datetime


@dataclass
class RiskEvent:
    strategy_id: Optional[int] = None
    rule: str = ""
    stock_code: Optional[str] = None
    bar_time: Optional[datetime] = None


@dataclass
class OrderEvent:
    strategy_id: int
    portfolio_id: int
    stock_code: str
    trade_type: TradeType
    signal_type: SignalType
    quantity: int
    price: Optional[Decimal] = None
    bar_time: Optional[datetime] = None
    # 触发该订单的信号名（公式变量名如 open_sig，风控如 stop_loss/take_profit/trailing_stop）
    signal_name: str = ""
    # 信号所属 bar 的日期（仅日线级 1d/1w/1mon 信号设置）。
    #   None = 分钟级信号，下一 bar open 立即成交（现有行为）。
    #   非 None = 日线级信号，须在 t.date() > signal_bar_date 的下一交易日 bar open 成交，
    #   防止当日 00:00 日线 bar 的信号在当日 09:30 分钟 bar 成交（未来函数）。
    signal_bar_date: Optional[date] = None
    period: Optional[str] = None
    # 在途单寿命起点（2026-11-16）：订单首次未能成交的交易日。回测引擎用它按
    # 交易日累计「已顺延天数」，超过 pending_order_max_days 作废（防停牌/连板
    # 无限顺延后凭空补做）。随增量 checkpoint 序列化，跨段续跑仍受同一上限约束。
    pending_since: Optional[date] = None


@dataclass
class TradeEvent:
    strategy_id: int
    portfolio_id: int
    stock_code: str
    trade_type: TradeType
    price: Decimal
    quantity: int
    amount: Decimal
    commission: Decimal
    stamp_duty: Decimal
    trade_time: datetime
    signal_type: Optional[SignalType] = None
    # 触发该成交的信号名，由 OrderEvent 透传（供交易明细定位来源）
    signal_name: str = ""
    # 过户费（双向，2026-11-11）：回测由 SimulatedDispatcher 按组合
    # transfer_fee_rate 计算；实盘柜台已并入真实费用，缺省 0。
    transfer_fee: Decimal = Decimal("0")

    def scaled_to(self, quantity: int) -> "TradeEvent":
        """按**实际入账数量**重算金额与费用，供现金入账与持仓扣减同口径（P0-1）。

        卖出成交可能大于虚拟持仓（继承仓重启重放只有 SELL、人工先减仓后若在途
        卖单成交、缺虚拟仓卖出）。``Position.apply_trade`` 会把数量夹到实际扣减量
        并返回该量；调用方随后必须用本方法把 amount/各项费用等比缩到该量再交给
        ``Account.apply_trade``——否则按原始成交额全额入账会让虚拟现金凭空增加
        （净值/回撤分母被污染，且重启重放与在线不一致）。

        quantity 与自身相同 → 返回自身（不产生新对象，保回测零开销）。
        """
        if quantity == self.quantity:
            return self
        qty = max(0, int(quantity))
        if qty == 0:
            return replace(
                self, quantity=0, amount=Decimal("0"),
                commission=Decimal("0"), stamp_duty=Decimal("0"),
                transfer_fee=Decimal("0"),
            )
        ratio = Decimal(qty) / Decimal(self.quantity)

        def _scale(value) -> Decimal:
            return ((value or Decimal("0")) * ratio).quantize(Decimal("0.01"))

        return replace(
            self,
            quantity=qty,
            amount=self.price * qty,
            commission=_scale(self.commission),
            stamp_duty=_scale(self.stamp_duty),
            transfer_fee=_scale(self.transfer_fee),
        )
