"""TdxTqDispatcher 映射逻辑测试（用假 tqcenter 模块，不启动通达信）。

覆盖 Core 契约：委托/持仓/资金字段归一化（通达信字段名与 QMT 不同）、
下单链路（order_stock → 回查 Wtbh 认领）、无 remark 时的 legacy 匹配所需插入时间，
以及下单失败/半自动确认模式的明确报错。
"""
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest

from core.engine.http_bridge_dispatcher import BridgeOrderRejected
from core.engine.live.order_machine import OrderStateMachine
from core.engine.live.timing import now_shanghai
from core.engine.tdx_tq_dispatcher import TdxTqDispatcher, normalize_symbol
from tq_iquant_shared.constants import SignalType, TradeType

CONST = SimpleNamespace(
    STOCK_BUY=0, STOCK_SELL=1,
    PRICE_MY=0, PRICE_SJ=1, PRICE_ZTJ=2, PRICE_DTJ=3,
)


def _order(code="000001", flag=0, status=1, price=10.5, vol=1000,
           filled=0, ref="W001"):
    """通达信 query_stock_orders 原始行（字段名照手册）。"""
    return {
        "Wtbh": ref, "Code": code, "BSFlag": flag, "Status": status,
        "WtPrice": price, "WtVol": vol, "CjVol": filled, "CjPrice": price,
    }


class FakeTq:
    """假 tqcenter.tq：记录调用、返回预置数据。"""

    def __init__(self, account_id=1, asset=None, positions=None, orders=None,
                 order_result=0, msg="", subscribed=None, cancel_result=None):
        self.account_id = account_id
        self.asset = asset or {}
        self.positions = positions or []
        self.orders = orders or []
        self.order_result = order_result
        self.msg = msg
        self.subscribed = subscribed
        # A-1：撤单返回值（SDK 形态：dict ErrorId="0"=成功 / -1=失败）
        self.cancel_result = cancel_result if cancel_result is not None else {"ErrorId": "0"}
        # A-5：按轮返回委托表（模拟「下单后新单延迟出现在委托表」）；None=用 self.orders
        self.orders_rounds = None
        self._round = 0
        self.calls = []

    def stock_account(self, account="", account_type=""):
        self.calls.append(("stock_account", account, account_type))
        return self.account_id

    def get_subscribe_hq_stock_list(self):
        self.calls.append(("get_subscribe_hq_stock_list",))
        if isinstance(self.subscribed, Exception):
            raise self.subscribed
        return list(self.subscribed or [])

    def query_stock_asset(self, aid):
        self.calls.append(("query_stock_asset", aid))
        return self.asset

    def query_stock_positions(self, aid):
        self.calls.append(("query_stock_positions", aid))
        return self.positions

    def query_stock_orders(self, aid):
        self.calls.append(("query_stock_orders", aid))
        if self.orders_rounds is not None:
            rows = self.orders_rounds[min(self._round, len(self.orders_rounds) - 1)]
            self._round += 1
            return rows
        return self.orders

    def cancel_order_stock(self, **kwargs):
        self.calls.append(("cancel_order_stock", kwargs))
        if isinstance(self.cancel_result, Exception):
            raise self.cancel_result
        return self.cancel_result

    def order_stock(self, **kwargs):
        self.calls.append(("order_stock", kwargs))
        if isinstance(self.order_result, dict):
            return self.order_result
        if self.order_result == 0 and self.msg:
            return {"ErrorId": "0", "Msg": self.msg}
        return self.order_result


@pytest.fixture
def fake(monkeypatch):
    """注入假 tq + 假 tqconst。"""
    tq = FakeTq()
    monkeypatch.setattr("core.engine.tdx_tq_dispatcher.get_tq", lambda: tq)
    monkeypatch.setattr("core.engine.tdx_tq_dispatcher._tqconst", lambda: CONST)
    return tq


@pytest.fixture
def no_quote(monkeypatch):
    """隔离真实 TQ 行情：涨跌停校验取不到前收 → fail-open 跳过。

    本机通达信可能正在运行，不隔离会让 place_order 单测依赖真实行情
    （前收恰在涨跌停边界时机器相关地失败）。
    """
    monkeypatch.setattr("core.tq.data.TQData", lambda: FakeQuoteData(raw={}))


def test_normalize_symbol_exchange_suffix():
    assert normalize_symbol("000001") == "000001.SZ"
    assert normalize_symbol("600000") == "600000.SH"
    assert normalize_symbol("430047") == "430047.BJ"
    # 92xxxx 为北交所新股段（不得按 9 开头落 SH）
    assert normalize_symbol("920718") == "920718.BJ"
    assert normalize_symbol("000001.SZ") == "000001.SZ"


def test_connect_fails_with_actionable_error_when_account_handle_invalid(fake):
    fake.account_id = -1
    d = TdxTqDispatcher(account="123456")
    assert d.heartbeat() is False
    assert "账户句柄" in d.last_error
    assert ("stock_account", "123456", "stock") in fake.calls


def test_connect_reports_tdx_not_logged_in(monkeypatch):
    from core.tq.utils import TDXConnectionError

    def boom():
        raise TDXConnectionError("无法加载 tqcenter")

    monkeypatch.setattr("core.engine.tdx_tq_dispatcher.get_tq", boom)
    d = TdxTqDispatcher()
    assert d.heartbeat() is False
    assert "tqcenter" in d.last_error


def test_get_asset_normalizes_field_names(fake):
    fake.asset = {"Asset": 100000.0, "Cash": 20000.0, "MarketValue": 80000.0,
                  "TotalFreeze": 0.0}
    d = TdxTqDispatcher()
    out = d.get_asset()
    assert out["total_asset"] == 100000.0
    assert out["available"] == 20000.0
    assert out["market_value"] == 80000.0
    assert out["frozen"] == 0.0


def test_get_positions_keeps_zero_available(fake):
    """available=0 是 T+1 当日买入的合法锁定值，不能回退成持仓量。"""
    fake.positions = [
        {"Code": "000001", "TotalVol": 1000, "CanUseVol": 0, "Cbj": 10.0},
        {"Code": "600000", "TotalVol": 500, "CanUseVol": 500, "Cbj": 8.0,
         "MarketValue": 4100.0},
    ]
    d = TdxTqDispatcher()
    rows = d.query_positions()
    assert rows[0] == {"instrument": "000001", "exchange": "SZ", "volume": 1000,
                       "available": 0, "cost": 10.0, "price": 0.0, "name": ""}
    assert rows[1]["exchange"] == "SH"
    assert rows[1]["available"] == 500


def test_query_orders_maps_status_and_skips_cancel_rows(fake):
    fake.orders = [
        _order(status=3, filled=1000, ref="W001"),          # 全部成交
        _order(code="600000", flag=1, status=5, ref="W002"),  # 全部撤单(卖出)
        {**_order(ref="W003"), "BSFlag": -1},               # 撤单流水，跳过
    ]
    d = TdxTqDispatcher()
    rows = d.query_orders()
    assert len(rows) == 2
    o = rows[0]
    assert o["source"] == "BRIDGE"
    assert o["remark"] == ""            # 通达信无法写 remark
    assert o["order_ref"] == "W001"
    assert o["direction"] == 48         # 买
    assert o["status"] == 56            # 已成
    assert o["filled_volume"] == 1000
    assert rows[1]["direction"] == 49   # 卖
    assert rows[1]["status"] == 54      # 已撤
    # A-5（行为变更）：非本进程下单的委托也补记当前上海时间（重启单/手工单兜底
    # 窗口匹配可用），不再留空——留空会让状态机永远无法绑定该 ref → 180s 超时丢单。
    assert rows[0]["insert_date"] and rows[0]["insert_time"]


def test_place_order_resolves_ref_and_enables_legacy_match(fake, no_quote):
    d = TdxTqDispatcher(account="123456")
    fake.orders = [_order(status=1, ref="W900")]
    order = SimpleNamespace(
        strategy_id=1, portfolio_id=2, stock_code="000001",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN, signal_name="open_sig",
        quantity=1000, price=Decimal("10.5"), bar_time=datetime(2026, 10, 24, 9, 31),
    )
    trade = d.place_order(order)

    call = [c for c in fake.calls if c[0] == "order_stock"][0][1]
    assert call["stock_code"] == "000001.SZ"
    assert call["order_type"] == 0
    assert call["order_volume"] == 1000
    assert call["price_type"] == 0
    assert call["price"] == 10.5

    assert trade is not None
    assert trade.quantity == 1000
    assert trade.price == Decimal("10.5")

    # 本进程下单后：query_orders 带插入时间，query_deals 出累计成交行
    fake.orders = [_order(status=3, filled=1000, ref="W900")]
    row = d.query_orders()[0]
    assert row["order_ref"] == "W900"
    assert row["insert_date"] and row["insert_time"]
    deal = d.query_deals()[0]
    assert deal["volume"] == 1000
    assert deal["amount"] == pytest.approx(10500.0)

    # legacy 模糊匹配（supports_remark=False）能在时间窗内认领该单
    live_order = SimpleNamespace(
        stock_code="000001.SZ", trade_type="buy", quantity=1000,
        created_at=datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=5),
    )
    assert OrderStateMachine.match_legacy_fuzzy(live_order, [row], set()) == "W900"


def test_place_order_keeps_submitted_when_order_not_in_orders_table(fake, no_quote):
    """柜台已受理（ErrorId=0）但回查窗口内委托表未见该单 → **不得当拒单**。

    拒单会让引擎把订单标 rejected 并移出在途集合：真实在途单从此无 ref、
    无成交回填、无法撤单（虚拟持仓/现金与柜台脱节，SELL 还会二次卖出）。
    正确行为：返回 None + last_unconfirmed_order_id，引擎保持 submitted，
    由 deals 轮询 legacy 模糊认领，最坏 180s 超时收口。
    """
    d = TdxTqDispatcher()
    fake.orders = []
    order = SimpleNamespace(
        strategy_id=1, portfolio_id=2, stock_code="000001",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN, signal_name="open_sig",
        quantity=1000, price=Decimal("10.5"), bar_time=None,
    )
    fake.order_result = 0
    fake.msg = "已提交"
    assert d.place_order(order) is None
    assert d.last_unconfirmed_order_id
    assert "未认领" in d.last_error


def test_place_order_flags_half_auto_confirm_mode(fake, no_quote):
    """待确认态且自动确认失败 → 明确拒单文案（含「交易信号」定位提示）。"""

    class _FailConfirmer:
        def __init__(self):
            self.specs = []

        def snapshot_rows(self):
            return ()

        def confirm(self, spec, is_submitted=None, deadline=None,
                    on_clicked=None, baseline=None):
            self.specs.append(spec)
            return SimpleNamespace(ok=False, detail="未找到「交易信号」窗口",
                                   clicks=0, stale=0)

    confirmer = _FailConfirmer()
    d = TdxTqDispatcher(confirmer=confirmer)
    fake.orders = []
    fake.msg = "已发送信号至客户端，待用户确认！"
    order = SimpleNamespace(
        strategy_id=1, portfolio_id=2, stock_code="000001",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN, signal_name="open_sig",
        quantity=1000, price=Decimal("10.5"), bar_time=None,
    )
    with pytest.raises(BridgeOrderRejected) as ei:
        d.place_order(order)
    assert "交易信号" in str(ei.value)
    # 确认器收到本次委托的识别特征（代码/方向/数量）
    assert confirmer.specs and confirmer.specs[0].code6 == "000001"
    assert confirmer.specs[0].direction == "buy"
    assert confirmer.specs[0].volume == 1000


def test_place_order_keeps_submitted_when_clicked_but_not_claimed(fake, no_quote):
    """已点『发送』但 deadline 内委托表未见该单 → 不可判拒单（保持 submitted）。

    点击成功只代表信号已发送，委托表刷新可能滞后；判拒单会让引擎丢弃在途单
    跟踪，下一根 bar 可能重复报单（SELL 双重卖出）。
    """

    class _ClickedNoRef:
        def snapshot_rows(self):
            return ()

        def confirm(self, spec, is_submitted=None, deadline=None,
                    on_clicked=None, baseline=None):
            if on_clicked:
                on_clicked()
            return SimpleNamespace(
                ok=False, detail="已点击『发送』，但超时内委托表仍未出现该单",
                clicks=1, stale=0)

    d = TdxTqDispatcher(confirmer=_ClickedNoRef())
    fake.orders = []
    fake.msg = "已发送信号至客户端，待用户确认！"
    order = SimpleNamespace(
        strategy_id=1, portfolio_id=2, stock_code="000001",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN, signal_name="open_sig",
        quantity=1000, price=Decimal("10.5"), bar_time=None,
    )
    assert d.place_order(order) is None
    assert d.last_unconfirmed_order_id
    assert "未认领" in d.last_error


def test_place_order_pending_with_auto_confirm_off_raises(fake, no_quote):
    """关闭自动确认（tq_trade.auto_confirm_dialog=false）→ 明确报错提示手动发送。"""
    d = TdxTqDispatcher(auto_confirm_dialog=False)
    fake.orders = []
    fake.msg = "已发送信号至客户端，待用户确认！"
    order = SimpleNamespace(
        strategy_id=1, portfolio_id=2, stock_code="000001",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN, signal_name="open_sig",
        quantity=1000, price=Decimal("10.5"), bar_time=None,
    )
    with pytest.raises(BridgeOrderRejected) as ei:
        d.place_order(order)
    assert "自动确认已关闭" in str(ei.value)


def test_place_order_auto_confirms_and_returns_trade(fake, no_quote):
    """待确认态 → 自动点『发送』→ 委托表出现该单 → 正常返回成交事件。"""

    class _OkConfirmer:
        def __init__(self, after_click):
            self.after_click = after_click
            self.specs = []

        def snapshot_rows(self):
            return ()

        def confirm(self, spec, is_submitted=None, deadline=None,
                    on_clicked=None, baseline=None):
            self.specs.append(spec)
            self.after_click()
            ok = bool(is_submitted and is_submitted())
            return SimpleNamespace(ok=ok, detail="已点击发送", clicks=1, stale=0)

    fake.orders = []
    fake.msg = "已发送信号至客户端，待用户确认！"
    confirmer = _OkConfirmer(lambda: fake.orders.append(
        _order(code="000001", flag=0, ref="W777", price=10.5, vol=1000)))
    d = TdxTqDispatcher(confirmer=confirmer)
    order = SimpleNamespace(
        strategy_id=1, portfolio_id=2, stock_code="000001",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN, signal_name="open_sig",
        quantity=1000, price=Decimal("10.5"), bar_time=None,
    )
    trade = d.place_order(order)
    assert trade is not None
    assert trade.quantity == 1000
    assert "W777" in d._claimed_refs


def _confirm_stub(order_holder):
    """自动确认桩：记录 SignalSpec，点击后把委托塞进委托表（模拟柜台认领）。"""

    class _Stub:
        def __init__(self):
            self.specs = []

        def snapshot_rows(self):
            return ()

        def confirm(self, spec, is_submitted=None, deadline=None,
                    on_clicked=None, baseline=None):
            self.specs.append(spec)
            order_holder.append(
                _order(code="000001", flag=0, ref="W888", price=10.5, vol=1000))
            ok = bool(is_submitted and is_submitted())
            return SimpleNamespace(ok=ok, detail="已点击发送", clicks=1, stale=0)

    return _Stub()


def _pending_order():
    return SimpleNamespace(
        strategy_id=1, portfolio_id=2, stock_code="000001",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN, signal_name="open_sig",
        quantity=1000, price=Decimal("10.5"), bar_time=None,
    )


def test_confirm_spec_carries_daily_price_limits(fake, no_quote):
    """确认时把本通道的当日涨跌停填进 SignalSpec（价格列 ROI 约束用）。"""
    fake.orders = []
    fake.msg = "已发送信号至客户端，待用户确认！"
    confirmer = _confirm_stub(fake.orders)
    d = TdxTqDispatcher(confirmer=confirmer)
    monkeypatch_result = (10.0, 11.0, 9.0)
    d.price_limit_bounds = lambda symbol: monkeypatch_result

    trade = d.place_order(_pending_order())

    assert trade is not None
    spec = confirmer.specs[0]
    assert spec.limit_up == 11.0
    assert spec.limit_down == 9.0


def test_confirm_proceeds_when_price_limits_unavailable(fake, no_quote):
    """取不到涨跌停 → 留空（0）且确认流程照常继续（fail-open 不拒单）。"""
    fake.orders = []
    fake.msg = "已发送信号至客户端，待用户确认！"
    confirmer = _confirm_stub(fake.orders)
    d = TdxTqDispatcher(confirmer=confirmer)
    d.price_limit_bounds = lambda symbol: None

    trade = d.place_order(_pending_order())

    assert trade is not None
    spec = confirmer.specs[0]
    assert spec.limit_up == 0.0
    assert spec.limit_down == 0.0


def test_place_order_skips_confirm_when_value_2(fake, no_quote):
    """Value=2（直接成功）不进入 UI 确认路径。"""

    class _SpyConfirmer:
        def __init__(self):
            self.called = False

        def snapshot_rows(self):
            return ()

        def confirm(self, *a, **kw):
            self.called = True
            return SimpleNamespace(ok=True, detail="", clicks=0, stale=0)

    spy = _SpyConfirmer()
    fake.order_result = {"ErrorId": "0", "Value": 2, "Msg": "委托成功"}
    fake.orders = [_order(code="000001", flag=0, ref="W888", price=10.5, vol=1000)]
    d = TdxTqDispatcher(confirmer=spy)
    order = SimpleNamespace(
        strategy_id=1, portfolio_id=2, stock_code="000001",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN, signal_name="open_sig",
        quantity=1000, price=Decimal("10.5"), bar_time=None,
    )
    assert d.place_order(order) is not None
    assert spy.called is False


def test_place_order_failure_returns_rejected(fake, no_quote):
    d = TdxTqDispatcher()
    fake.order_result = -1
    order = SimpleNamespace(
        strategy_id=1, portfolio_id=2, stock_code="000001",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN, signal_name="open_sig",
        quantity=1000, price=Decimal("10.5"), bar_time=None,
    )
    with pytest.raises(BridgeOrderRejected):
        d.place_order(order)


def test_place_order_error_id_raises(monkeypatch, no_quote):
    tq = FakeTq(order_result={"ErrorId": "5", "Error": "资金不足"})
    monkeypatch.setattr("core.engine.tdx_tq_dispatcher.get_tq", lambda: tq)
    monkeypatch.setattr("core.engine.tdx_tq_dispatcher._tqconst", lambda: CONST)
    d = TdxTqDispatcher()
    order = SimpleNamespace(
        strategy_id=1, portfolio_id=2, stock_code="600000",
        trade_type=TradeType.SELL, signal_type=SignalType.CLOSE, signal_name="close_sig",
        quantity=500, price=Decimal("8.0"), bar_time=None,
    )
    with pytest.raises(BridgeOrderRejected) as ei:
        d.place_order(order)
    assert "资金不足" in str(ei.value)


def test_query_calendar_and_quote_degrade_to_empty(monkeypatch):
    """TQ 数据不可用时不抛异常（交易日历 fail-open，行情返回空）。"""
    d = TdxTqDispatcher()
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: SimpleNamespace(get_history_raw=lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no tdx"))),
    )
    assert d.query_calendar() == []
    assert d.query_quote("000001") == []
    assert "unavailable" in d.last_error


# ---------------- TQ 1.3.1：交易日历（权威 get_trading_dates + 日线反推回退） ----------------

class FakeTQData:
    """假 TQData：预置 get_trading_dates / get_history_raw / unsubscribe_bars 行为。"""

    def __init__(self, trading_dates=None, history_raw=None,
                 raise_dates=False, raise_history=False):
        self._trading_dates = trading_dates
        self._history_raw = history_raw
        self._raise_dates = raise_dates
        self._raise_history = raise_history
        self.cal_args = None
        self.history_calls = []
        self.unsub_calls = []

    def get_trading_dates(self, market="SH", start="", end=""):
        self.cal_args = (market, start, end)
        if self._raise_dates:
            raise RuntimeError("calendar boom")
        return list(self._trading_dates or [])

    def get_history_raw(self, stocks, periods, **kw):
        self.history_calls.append((list(stocks), list(periods), kw))
        if self._raise_history:
            raise RuntimeError("history boom")
        return self._history_raw or {}

    def unsubscribe_bars(self, stocks):
        self.unsub_calls.append(list(stocks))


class FakeQuoteData:
    """假 TQData（仅行情）：get_history_raw 返回预置 raw 或抛错。"""

    def __init__(self, raw=None, error=None):
        self._raw = raw
        self._error = error
        self.history_calls = []

    def get_history_raw(self, stocks, periods, **kw):
        self.history_calls.append((list(stocks), list(periods), kw))
        if self._error is not None:
            raise self._error
        return self._raw or {}


def test_query_calendar_prefers_trading_dates(monkeypatch):
    """get_trading_dates 命中 → 直接返回，不再走 000001.SH 日线反推。"""
    tqd = FakeTQData(trading_dates=["20261023", "20261026"], raise_history=True)
    monkeypatch.setattr("core.tq.data.TQData", lambda: tqd)
    d = TdxTqDispatcher()
    assert d.query_calendar() == ["20261023", "20261026"]
    assert tqd.history_calls == []          # 未走日线反推回退
    market, start, end = tqd.cal_args
    assert market == "SH"
    assert len(start) == 8 and len(end) == 8 and start < end


def test_query_calendar_falls_back_to_daily_index(monkeypatch):
    """get_trading_dates 空 → 回退 000001.SH 日线反推（1.3.1 前口径）。"""
    idx = [pd.Timestamp("2026-10-21"), pd.Timestamp("2026-10-22"),
           pd.Timestamp("2026-10-23")]
    raw = {"1d": {"Close": pd.DataFrame({"000001.SH": [1.0, 1.1, 1.2]}, index=idx)}}
    tqd = FakeTQData(trading_dates=[], history_raw=raw)
    monkeypatch.setattr("core.tq.data.TQData", lambda: tqd)
    d = TdxTqDispatcher()
    assert d.query_calendar() == ["20261021", "20261022", "20261023"]
    assert tqd.history_calls and tqd.history_calls[0][0] == ["000001.SH"]


def test_query_calendar_all_fail_returns_empty(monkeypatch):
    """两层都失败 → [] 且 last_error（fail-open 契约不变）。"""
    tqd = FakeTQData(raise_dates=True, raise_history=True)
    monkeypatch.setattr("core.tq.data.TQData", lambda: tqd)
    d = TdxTqDispatcher()
    assert d.query_calendar() == []
    assert "unavailable" in d.last_error


# ---------------- TQ 1.3.1：close() 退订对账 ----------------

def test_close_unsubscribes_current_subscriptions(fake, monkeypatch, caplog):
    """订阅列表非空 → 整表退订 + info 日志。

    前提：平台全局限 1 个运行 session（B6），当前订阅必属本会话，不会误伤。
    """
    fake.subscribed = ["000001.SZ", "600000.SH"]
    tqd = FakeTQData()
    monkeypatch.setattr("core.tq.data.TQData", lambda: tqd)
    d = TdxTqDispatcher()
    d._connected = True
    with caplog.at_level(logging.INFO, logger="core.engine.tdx_tq_dispatcher"):
        d.close()
    assert tqd.unsub_calls == [["000001.SZ", "600000.SH"]]
    assert d._connected is False
    assert any(r.levelno == logging.INFO and "退订" in r.message
               for r in caplog.records)


def test_close_empty_subscription_skips_unsubscribe(fake, monkeypatch):
    """订阅列表空 → 不调 unsubscribe_bars。"""
    fake.subscribed = []
    tqd = FakeTQData()
    monkeypatch.setattr("core.tq.data.TQData", lambda: tqd)
    d = TdxTqDispatcher()
    d.close()
    assert tqd.unsub_calls == []
    assert d._connected is False


def test_close_swallows_subscription_query_error(fake, monkeypatch):
    """查订阅列表异常 → 吞并（debug 日志），不抛、不退订。"""
    fake.subscribed = RuntimeError("bridge down")
    tqd = FakeTQData()
    monkeypatch.setattr("core.tq.data.TQData", lambda: tqd)
    d = TdxTqDispatcher()
    d._connected = True
    d.close()  # 不抛
    assert tqd.unsub_calls == []
    assert d._connected is False


def test_close_swallows_unsubscribe_error(fake, monkeypatch):
    """unsubscribe_bars 异常 → 吞并，不抛。"""
    fake.subscribed = ["000001.SZ"]
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: SimpleNamespace(
            unsubscribe_bars=lambda stocks: (_ for _ in ()).throw(RuntimeError("unsub boom"))),
    )
    d = TdxTqDispatcher()
    d.close()  # 不抛


# ---------------- TQ 1.3.1：check_price_limit 涨跌停前置校验（引擎接线口径）----------------
# Fix1 口径统一：校验从 place_order 内联上移为公共方法（引擎按
# price_limit_check_enabled 门控调用，四通道同签名），place_order 不再内联校验。

def _raw_1d_close(symbol, closes, days=10):
    """1d 原始行情（仅 Close 帧，prev_close 计算够用）。

    最新 bar 固定为今日（新鲜度校验要求最新 bar == 今天，写死历史日期会被
    当成数据滞后跳过校验）。默认补到 10 根：涨跌停推算多取几根判「上市交易日序」
    （新股上市初期无涨跌幅限制），只有 2 根会被当成新股而整段跳过校验。
    """
    today = now_shanghai().date()
    values = list(closes)
    if len(values) < days:
        values = [values[0]] * (days - len(values)) + values
    idx = [pd.Timestamp(today - timedelta(days=days - 1 - i))
           for i in range(days)]
    return {"1d": {"Close": pd.DataFrame({symbol: values}, index=idx)}}


def _limit_order(code="000001", trade_type=TradeType.BUY, price=Decimal("11.00")):
    return SimpleNamespace(
        strategy_id=1, portfolio_id=2, stock_code=code,
        trade_type=trade_type, signal_type=SignalType.OPEN, signal_name="open_sig",
        quantity=1000, price=price, bar_time=None,
    )


def test_check_price_limit_blocks_buy_at_limit_up(fake, monkeypatch):
    """限价买 ≥ 涨停价（前收 10 × 1.10 = 11.00）→ 返回拒绝原因，不触 order_stock。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("000001.SZ", [10.0, 10.0])))
    d = TdxTqDispatcher()
    reason = d.check_price_limit("000001", "buy", Decimal("11.00"))
    assert reason is not None
    assert "涨停价" in reason
    assert "11.00" in reason
    assert not [c for c in fake.calls if c[0] == "order_stock"]


def test_check_price_limit_blocks_sell_at_limit_down(fake, monkeypatch):
    """限价卖 ≤ 跌停价（前收 10 × 0.90 = 9.00）→ 返回拒绝原因，不触 order_stock。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("000001.SZ", [10.0, 10.0])))
    d = TdxTqDispatcher()
    reason = d.check_price_limit("000001", "sell", Decimal("9.00"))
    assert reason is not None
    assert "跌停价" in reason
    assert "9.00" in reason
    assert not [c for c in fake.calls if c[0] == "order_stock"]


def test_place_order_buy_below_limit_up_passes(fake, monkeypatch):
    """限价买 < 涨停价 → 校验通过（None）且正常下单。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("000001.SZ", [10.0, 10.0])))
    fake.orders = [_order(price=10.99, ref="W801")]
    d = TdxTqDispatcher()
    assert d.check_price_limit("000001", "buy", Decimal("10.99")) is None
    trade = d.place_order(_limit_order(price=Decimal("10.99")))
    assert trade is not None
    assert [c for c in fake.calls if c[0] == "order_stock"]


def test_place_order_sell_above_limit_down_passes(fake, monkeypatch):
    """限价卖 > 跌停价 → 校验通过（None）且正常下单。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("000001.SZ", [10.0, 10.0])))
    fake.orders = [_order(flag=1, price=9.01, ref="W802")]
    d = TdxTqDispatcher()
    assert d.check_price_limit("000001", "sell", Decimal("9.01")) is None
    trade = d.place_order(
        _limit_order(trade_type=TradeType.SELL, price=Decimal("9.01")))
    assert trade is not None
    assert [c for c in fake.calls if c[0] == "order_stock"]


def test_place_order_no_inline_limit_check(fake, monkeypatch):
    """防双查：涨停价限价单 place_order 直接放行（校验已上移引擎提交链，
    由 price_limit_check_enabled 配置门控调用 check_price_limit）。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("000001.SZ", [10.0, 10.0])))
    fake.orders = [_order(price=11.00, ref="W807")]
    d = TdxTqDispatcher()
    assert d.check_price_limit("000001", "buy", Decimal("11.00")) is not None
    trade = d.place_order(_limit_order(price=Decimal("11.00")))
    assert trade is not None
    assert [c for c in fake.calls if c[0] == "order_stock"]


def test_check_price_limit_market_price_skips(fake, monkeypatch):
    """市价单（price 空/非正，成交价由交易所决定）不做涨跌停校验 → None。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("000001.SZ", [10.0, 10.0])))
    d = TdxTqDispatcher()
    assert d.check_price_limit("000001", "buy", None) is None
    assert d.check_price_limit("000001", "buy", 0) is None


def test_check_price_limit_quote_error_fail_open(fake, monkeypatch):
    """行情查询异常 → 返回 None（fail-open，校验故障绝不阻断下单）。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(error=RuntimeError("no tdx")))
    fake.orders = [_order(price=11.00, ref="W804")]
    d = TdxTqDispatcher()
    assert d.check_price_limit("000001", "buy", Decimal("11.00")) is None
    trade = d.place_order(_limit_order(price=Decimal("11.00")))
    assert trade is not None


def test_check_price_limit_distorted_prev_close_fail_open(fake, monkeypatch):
    """前复权早期失真（前收 < 1 元）→ 返回 None（与回测同口径，宁宽勿错）。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("000001.SZ", [0.4, 0.5])))
    fake.orders = [_order(price=11.00, ref="W805")]
    d = TdxTqDispatcher()
    assert d.check_price_limit("000001", "buy", Decimal("11.00")) is None
    trade = d.place_order(_limit_order(price=Decimal("11.00")))
    assert trade is not None


def test_check_price_limit_stale_bar_skips(fake, monkeypatch):
    """最新 bar=昨日（数据滞后）→ 前收无效，返回 None（不拒单，fail-open）。"""
    today = now_shanghai().date()
    idx = [pd.Timestamp(today - timedelta(days=2)),
           pd.Timestamp(today - timedelta(days=1))]
    raw = {"1d": {"Close": pd.DataFrame({"000001.SZ": [10.0, 10.0]}, index=idx)}}
    monkeypatch.setattr(
        "core.tq.data.TQData", lambda: FakeQuoteData(raw))
    fake.orders = [_order(price=11.00, ref="W806")]
    d = TdxTqDispatcher()
    assert d.check_price_limit("000001", "buy", Decimal("11.00")) is None
    trade = d.place_order(_limit_order(price=Decimal("11.00")))
    assert trade is not None
    assert [c for c in fake.calls if c[0] == "order_stock"]


def test_check_price_limit_fresh_bar_still_blocks_limit_up(fake, monkeypatch):
    """最新 bar=今日（数据新鲜）→ 涨跌停校验照常生效（拒单口径不变）。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("000001.SZ", [10.0, 10.0])))
    d = TdxTqDispatcher()
    reason = d.check_price_limit("000001", "buy", Decimal("11.00"))
    assert reason is not None
    assert "涨停价" in reason
    assert not [c for c in fake.calls if c[0] == "order_stock"]


# ---------------- 涨跌停推算：板块 + ST + 新股上市初期（core.engine.price_limit）----------------

def _bars(symbol, closes):
    """指定收盘序列的 1d 原始行情（末日 = 今天），用于构造上市日数/新股场景。"""
    today = now_shanghai().date()
    n = len(closes)
    idx = [pd.Timestamp(today - timedelta(days=n - 1 - i)) for i in range(n)]
    return {"1d": {"Close": pd.DataFrame({symbol: list(closes)}, index=idx)}}


def _patch_name(monkeypatch, name):
    monkeypatch.setattr(
        TdxTqDispatcher, "_stock_name", lambda self, symbol: name)


def test_check_price_limit_main_st_uses_5pct(fake, monkeypatch):
    """主板 ST 幅度 5%（不是 10%）：10.50 = 前收 10 × 1.05 → 拦；10.49 放行。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("600000.SH", [10.0, 10.0])))
    _patch_name(monkeypatch, "ST浦发")
    d = TdxTqDispatcher()
    reason = d.check_price_limit("600000", "buy", Decimal("10.50"))
    assert reason is not None
    assert "涨停价" in reason
    assert "source=name_st" in reason and "confidence=high" in reason
    assert d.check_price_limit("600000", "buy", Decimal("10.49")) is None


def test_check_price_limit_chinext_st_keeps_20pct(fake, monkeypatch):
    """创业板 ST 不降档（仍 20%）：11.50 放行，12.00 才拦（旧实现按 10% 会误拦）。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("300750.SZ", [10.0, 10.0])))
    _patch_name(monkeypatch, "*ST宁德")
    d = TdxTqDispatcher()
    assert d.check_price_limit("300750", "buy", Decimal("11.50")) is None
    reason = d.check_price_limit("300750", "buy", Decimal("12.00"))
    assert reason is not None and "12.00" in reason


def test_check_price_limit_star_20pct(fake, monkeypatch):
    """科创板 20%（ST 同样 20%）：11.99 放行、12.00 拦。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("688981.SH", [10.0, 10.0])))
    _patch_name(monkeypatch, "*ST中芯")
    d = TdxTqDispatcher()
    assert d.check_price_limit("688981", "buy", Decimal("11.99")) is None
    assert d.check_price_limit("688981", "buy", Decimal("12.00")) is not None


def test_check_price_limit_bse_30pct(fake, monkeypatch):
    """北交所 30%：12.99 放行、13.00 拦（旧实现对 8 开头已正确，此处锁定不回归）。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("830799.BJ", [10.0, 10.0])))
    _patch_name(monkeypatch, "艾融软件")
    d = TdxTqDispatcher()
    assert d.check_price_limit("830799", "buy", Decimal("12.99")) is None
    reason = d.check_price_limit("830799", "buy", Decimal("13.00"))
    assert reason is not None and "board=bse" in reason


def test_check_price_limit_new_listing_first_5_days_skips(fake, monkeypatch):
    """主板/创业板/科创板新股前 5 个交易日无涨跌幅限制 → 本地不拦（fail-open）。

    5 根日线 = 今日为上市第 5 个交易日（前收那根之前仅 4 根）→ 仍在窗口内。
    """
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_bars("600001.SH", [10.0] * 5)))
    _patch_name(monkeypatch, "新股甲")
    d = TdxTqDispatcher()
    # 11.50 超过 10% 幅度，若是老股必拦；新股窗口内应放行（交柜台）
    assert d.check_price_limit("600001", "buy", Decimal("11.50")) is None


def test_check_price_limit_new_listing_window_ends_on_day6(fake, monkeypatch):
    """第 6 个交易日恢复 10%：6 根日线（前收前 5 根）→ 11.00 涨停价照常拦。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_bars("600001.SH", [10.0] * 6)))
    _patch_name(monkeypatch, "新股甲")
    d = TdxTqDispatcher()
    reason = d.check_price_limit("600001", "buy", Decimal("11.00"))
    assert reason is not None and "涨停价" in reason


def test_check_price_limit_stale_local_bars_not_treated_as_new_listing(fake, monkeypatch):
    """本地日线残缺（仅 3 根但最早一根在 100 天前）→ 不判新股，按板块默认幅度。"""
    today = now_shanghai().date()
    idx = [pd.Timestamp(today - timedelta(days=d)) for d in (100, 60, 0)]
    raw = {"1d": {"Close": pd.DataFrame({"600002.SH": [10.0, 10.0, 10.0]},
                                       index=idx)}}
    monkeypatch.setattr("core.tq.data.TQData", lambda: FakeQuoteData(raw))
    _patch_name(monkeypatch, "老股乙")
    d = TdxTqDispatcher()
    reason = d.check_price_limit("600002", "buy", Decimal("11.00"))
    assert reason is not None and "board=main" in reason


def test_check_price_limit_fund_skips_local_check(fake, monkeypatch):
    """ETF 幅度随标的（10%/20%）仅凭代码不可定档 → fail-open 交柜台。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("510050.SH", [10.0, 10.0])))
    _patch_name(monkeypatch, "50ETF")
    d = TdxTqDispatcher()
    assert d.check_price_limit("510050", "buy", Decimal("11.00")) is None


def test_check_price_limit_name_unknown_uses_board_default(fake, monkeypatch):
    """名称取不到 → 按板块默认幅度推（主板 10%）并标注 name_unknown/medium。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("000001.SZ", [10.0, 10.0])))
    _patch_name(monkeypatch, None)
    d = TdxTqDispatcher()
    reason = d.check_price_limit("000001", "buy", Decimal("11.00"))
    assert reason is not None
    assert "source=board_default_name_unknown" in reason
    assert "confidence=medium" in reason


def test_price_limit_bounds_main_st_5pct(fake, monkeypatch):
    """盘前涨跌停：主板 ST → 前收 10 的 10.50 / 9.50。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("600000.SH", [10.0])))
    _patch_name(monkeypatch, "ST浦发")
    d = TdxTqDispatcher()
    assert d.price_limit_bounds("600000") == (10.0, 10.5, 9.5)


def test_price_limit_bounds_new_listing_returns_none(fake, monkeypatch):
    """新股无涨跌幅限制 → 不夹价（None）。"""
    monkeypatch.setattr(
        "core.tq.data.TQData", lambda: FakeQuoteData(_bars("300999.SZ", [10.0] * 3)))
    _patch_name(monkeypatch, "新股丙")
    d = TdxTqDispatcher()
    assert d.price_limit_bounds("300999") is None


def test_price_limit_bounds_fund_returns_none(fake, monkeypatch):
    """ETF 无可靠幅度 → 不夹价（None）。"""
    monkeypatch.setattr(
        "core.tq.data.TQData",
        lambda: FakeQuoteData(_raw_1d_close("510050.SH", [10.0])))
    _patch_name(monkeypatch, "50ETF")
    d = TdxTqDispatcher()
    assert d.price_limit_bounds("510050") is None


def test_stock_name_cached_per_day(fake, monkeypatch):
    """名称按日缓存：同一天第二次调用不再打 TQ 信息接口。"""
    calls = []

    class _Info(FakeQuoteData):
        def get_market_snapshot_batch(self, stocks, fields):
            calls.append(list(stocks))
            return {stocks[0]: {"Name": "ST测试"}}

    monkeypatch.setattr("core.tq.data.TQData", lambda: _Info({}))
    d = TdxTqDispatcher()
    assert d._stock_name("600000.SH") == "ST测试"
    assert d._stock_name("600000.SH") == "ST测试"
    assert len(calls) == 1


# ---------------- TQ 1.3.1：批量行情 query_quotes_batch ----------------

def _raw_1m_multi():
    """1m 原始行情：两股票 × Close/Open/Volume 帧。"""
    idx = [pd.Timestamp("2026-10-24 09:31:00"), pd.Timestamp("2026-10-24 09:32:00")]

    def frame(data):
        return pd.DataFrame(data, index=idx)

    return {"1m": {
        "Close": frame({"000001.SZ": [10.0, 10.1], "600000.SH": [8.0, 8.1]}),
        "Open": frame({"000001.SZ": [9.9, 10.0], "600000.SH": [7.9, 8.0]}),
        "Volume": frame({"000001.SZ": [100, 200], "600000.SH": [300, 400]}),
    }}


def test_supports_batch_quote_flag():
    """仅 TdxTqDispatcher 声明 supports_batch_quote=True。"""
    assert TdxTqDispatcher.supports_batch_quote is True


def test_query_quotes_batch_shape(monkeypatch):
    tqd = FakeQuoteData(_raw_1m_multi())
    monkeypatch.setattr("core.tq.data.TQData", lambda: tqd)
    d = TdxTqDispatcher()
    out = d.query_quotes_batch(["000001.SZ", "600000.SH"], period="1m", count=10)
    assert set(out.keys()) == {"000001.SZ", "600000.SH"}
    b = out["000001.SZ"]
    assert [x["stime"] for x in b] == ["20261024093100", "20261024093200"]
    assert b[0]["close"] == pytest.approx(10.0)
    assert b[1]["close"] == pytest.approx(10.1)
    assert b[0]["open"] == pytest.approx(9.9)
    assert b[0]["volume"] == pytest.approx(100)
    assert out["600000.SH"][1]["close"] == pytest.approx(8.1)
    # 一次批量 SDK 调用，两只股票一起取
    assert tqd.history_calls[0][0] == ["000001.SZ", "600000.SH"]
    assert tqd.history_calls[0][1] == ["1m"]


def test_query_quotes_batch_filters_invalid_codes(monkeypatch):
    """无效代码剔除（不进 SDK 调用），结果键保留原始入参。"""
    tqd = FakeQuoteData(_raw_1m_multi())
    monkeypatch.setattr("core.tq.data.TQData", lambda: tqd)
    d = TdxTqDispatcher()
    out = d.query_quotes_batch(["000001", "12", "xyz"], period="1m")
    assert set(out.keys()) == {"000001"}
    assert tqd.history_calls[0][0] == ["000001.SZ"]


def test_query_quotes_batch_error_returns_empty(monkeypatch):
    """批量查询异常 → {} + last_error（BarPoller 据此回退逐只）。"""
    tqd = FakeQuoteData(error=RuntimeError("no tdx"))
    monkeypatch.setattr("core.tq.data.TQData", lambda: tqd)
    d = TdxTqDispatcher()
    assert d.query_quotes_batch(["000001.SZ"]) == {}
    assert "batch" in d.last_error


def test_query_quote_consistent_with_batch(monkeypatch):
    """单股 query_quote 与批量结果同转换规则（共用 _raw_to_bars）。"""
    tqd = FakeQuoteData(_raw_1m_multi())
    monkeypatch.setattr("core.tq.data.TQData", lambda: tqd)
    d = TdxTqDispatcher()
    one = d.query_quote("000001", period="1m", count=10)
    batch = d.query_quotes_batch(["000001"], period="1m", count=10)
    assert one == batch["000001"]


# ---------------- A-1：cancel_order（真实撤单） ----------------

def _connected_dispatcher(**kw):
    """跳过 _ensure 的真连接（单测直连假 tq）。"""
    d = TdxTqDispatcher(**kw)
    d._connected = True
    d._account_id = 1
    return d


def test_cancel_order_success_returns_true(fake):
    """SDK 返回 dict(ErrorId="0") → True，参数带后缀代码。"""
    d = _connected_dispatcher()
    assert d.cancel_order("W001", "000001") is True
    call = [c for c in fake.calls if c[0] == "cancel_order_stock"][0][1]
    assert call == {"account_id": 1, "stock_code": "000001.SZ", "order_id": "W001"}


def test_cancel_order_minus_one_returns_false_with_error(fake):
    """SDK 返回 -1（柜台失败/参数缺失）→ False + last_error。"""
    fake.cancel_result = -1
    d = _connected_dispatcher()
    assert d.cancel_order("W001", "000001.SZ") is False
    assert d.last_error


def test_cancel_order_error_id_dict_returns_false(fake):
    """SDK 返回 dict(ErrorId≠0) → False + last_error（防 SDK 形态变化）。"""
    fake.cancel_result = {"ErrorId": "6", "Msg": "连接已断开"}
    d = _connected_dispatcher()
    assert d.cancel_order("W001", "000001.SZ") is False
    assert "连接已断开" in d.last_error


def test_cancel_order_connection_error_propagates(monkeypatch):
    """TDXConnectionError（客户端离线）向上抛，不吞。"""
    from core.tq.utils import TDXConnectionError

    def boom():
        raise TDXConnectionError("无法加载 tqcenter")

    monkeypatch.setattr("core.engine.tdx_tq_dispatcher.get_tq", boom)
    d = _connected_dispatcher()
    with pytest.raises(TDXConnectionError):
        d.cancel_order("W001", "000001.SZ")


# ---------------- A-5：_resolve_order_id 认领加固 ----------------

def test_get_orders_merges_known_refs_and_backfills_submit_times(fake):
    """A-5：get_orders 返回后 refs 并入 _known_refs，缺 _submit_times 的 ref 用当前上海时间补记。"""
    d = _connected_dispatcher()
    fake.orders = [_order(ref="W001"), _order(code="600000", ref="W002")]
    d.get_orders()
    assert d._known_refs == {"W001", "W002"}
    assert set(d._submit_times) == {"W001", "W002"}
    date_s, time_s = d._submit_times["W001"]
    assert date_s == now_shanghai().strftime("%Y%m%d")
    assert len(time_s) == 6


def test_resolve_prefers_new_ref_over_known_same_shape(fake):
    """A-5：同形状候选中优先认领本轮新增 ref（预置 known 的旧单不冒领）。"""
    d = _connected_dispatcher()
    fake.orders = [_order(ref="W100")]
    d.get_orders()  # 预热：W100 进入 _known_refs（模拟此前轮询已见过）
    fake.orders_rounds = [
        [_order(ref="W100"), _order(ref="W200")],
    ]
    ref = d._resolve_order_id("000001.SZ", "buy", 10.5, 1000, attempts=1)
    assert ref == "W200"
    assert "W200" in d._claimed_refs


def test_resolve_skips_claimed_refs(fake):
    """A-5：已被本进程其它单认领的 ref 不重复认领。"""
    d = _connected_dispatcher()
    d._claimed_refs.add("W100")
    fake.orders = [_order(ref="W100")]
    assert d._resolve_order_id("000001.SZ", "buy", 10.5, 1000, attempts=1) == ""


def test_resolve_falls_back_to_known_unclaimed_ref(fake):
    """A-5：全部 attempts 无新增 ref 命中 → 放宽为形状匹配且未认领的任意 ref（兜底）。"""
    d = _connected_dispatcher()
    fake.orders = [_order(ref="W100")]
    d.get_orders()  # W100 已知（非本轮新增）
    ref = d._resolve_order_id("000001.SZ", "buy", 10.5, 1000, attempts=1)
    assert ref == "W100"
    assert "W100" in d._claimed_refs


def test_resolve_market_mode_skips_price_filter(fake):
    """A-5：市价模式（latest→PRICE_SJ）委托表 WtPrice 可能为 0/非传入价 → 跳过价格过滤。"""
    d = _connected_dispatcher(price_mode="latest")
    fake.orders = [_order(price=0.0, ref="W300")]
    ref = d._resolve_order_id("000001.SZ", "buy", 10.5, 1000, attempts=1)
    assert ref == "W300"


def test_resolve_limit_mode_keeps_price_filter(fake):
    """A-5：自填价模式（my→PRICE_MY）保持价格相等过滤（错价旧单不认领）。"""
    d = _connected_dispatcher(price_mode="my")
    fake.orders = [_order(price=9.9, ref="W301")]
    assert d._resolve_order_id("000001.SZ", "buy", 10.5, 1000, attempts=1) == ""


def test_resolve_known_refs_survive_reconnect(fake):
    """A-5：connect() 重连不清空 _claimed_refs/_known_refs（进程内防重放）。"""
    d = _connected_dispatcher()
    fake.orders = [_order(ref="W100")]
    d.get_orders()
    d._claimed_refs.add("W100")
    d.connect()  # fake account_id=1 → 重连成功
    assert d._known_refs == {"W100"}
    assert "W100" in d._claimed_refs


def test_query_orders_backfills_insert_time_for_foreign_refs(fake):
    """A-5（行为变更）：非本进程下单的委托也补记当前上海时间——重启单/手工单兜底认领后
    状态机 legacy 窗口匹配可用（旧行为留空 → 状态机永远无法绑定 → 180s 超时丢单）。"""
    fake.orders = [_order(status=3, filled=1000, ref="W001")]
    d = _connected_dispatcher()
    rows = d.query_orders()
    assert rows[0]["insert_date"] and rows[0]["insert_time"]


# ---------------- I-2：陌生 ref 补记时间标 fabricated，匹配降级 ----------------

def test_get_orders_marks_fabricated_refs(fake):
    """I-2：纯陌生 ref（_submit_times 没有）的补记时间标 fabricated；
    已有时间的 ref（本进程 place_order / A-4 recover 补记）不标。"""
    d = _connected_dispatcher()
    d._submit_times["W900"] = ("20261027", "093000")  # 预置真实时间
    fake.orders = [_order(ref="W001"), _order(ref="W900")]
    d.get_orders()
    assert d._fabricated_refs == {"W001"}
    assert "W900" not in d._fabricated_refs


def test_query_orders_rows_carry_insert_fabricated_flag(fake):
    """I-2：query_orders 每行带 insert_fabricated，状态机据此拒绝 fabricated 候选。"""
    fake.orders = [_order(status=3, filled=1000, ref="W001")]
    d = _connected_dispatcher()
    rows = d.query_orders()
    assert rows[0]["insert_fabricated"] is True
    assert rows[0]["insert_date"] and rows[0]["insert_time"]


def test_resolve_fallback_prefers_real_time_over_fabricated(fake):
    """I-2：兜底段真实时间候选优先，fabricated（遗留单/手工单）沉底——仅无真实候选时才用。"""
    d = _connected_dispatcher()
    fake.orders = [_order(ref="W100"), _order(ref="W101")]
    d._submit_times["W101"] = ("20261027", "100000")  # 真实时间（本进程单未完结）
    d.get_orders()  # W100 陌生 → fabricated；W101 已有真实时间
    ref = d._resolve_order_id("000001.SZ", "buy", 10.5, 1000, attempts=1)
    assert ref == "W101"
    assert "W100" not in d._claimed_refs


def test_resolve_fallback_uses_fabricated_as_last_resort(fake):
    """I-2：只有 fabricated 候选时兜底仍可用（既有行为保留，但排在真实候选之后）。"""
    d = _connected_dispatcher()
    fake.orders = [_order(ref="W100")]
    d.get_orders()  # W100 fabricated
    ref = d._resolve_order_id("000001.SZ", "buy", 10.5, 1000, attempts=1)
    assert ref == "W100"
    assert "W100" in d._claimed_refs


def test_place_order_clears_fabricated_flag_on_claim(fake, no_quote):
    """I-2：place_order 认领的 ref 清除 fabricated 标记（resolve 回查期间 get_orders
    可能已把它当陌生 ref 补记），本进程单的 legacy 匹配不受降级影响。"""
    d = TdxTqDispatcher(account="123456")
    fake.orders = [_order(status=1, ref="W900")]
    order = SimpleNamespace(
        strategy_id=1, portfolio_id=2, stock_code="000001",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN, signal_name="open_sig",
        quantity=1000, price=Decimal("10.5"), bar_time=datetime(2026, 10, 27, 9, 31),
    )
    trade = d.place_order(order)
    assert trade is not None
    assert "W900" not in d._fabricated_refs
    row = d.query_orders()[0]
    assert row["insert_fabricated"] is False


def test_match_legacy_fuzzy_rejects_fabricated_row():
    """I-2：状态机 legacy 匹配拒绝 insert_fabricated 候选（时间窗校验对补记时间无意义），
    真实时间候选正常绑定。"""
    live_order = SimpleNamespace(
        stock_code="000001.SZ", trade_type="buy", quantity=1000,
        created_at=datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=5),
    )
    fabricated = {
        "source": "BRIDGE", "instrument": "000001", "exchange": "SZ",
        "direction": 48, "volume": 1000, "order_ref": "W100",
        "insert_date": now_shanghai().strftime("%Y%m%d"),
        "insert_time": now_shanghai().strftime("%H%M%S"),
        "insert_fabricated": True,
    }
    real = {
        "source": "BRIDGE", "instrument": "000001", "exchange": "SZ",
        "direction": 48, "volume": 1000, "order_ref": "W200",
        "insert_date": now_shanghai().strftime("%Y%m%d"),
        "insert_time": now_shanghai().strftime("%H%M%S"),
        "insert_fabricated": False,
    }
    assert OrderStateMachine.match_legacy_fuzzy(live_order, [fabricated], set()) is None
    assert OrderStateMachine.match_legacy_fuzzy(live_order, [real], set()) == "W200"


# ---------------- 价格归属处置：PRICE_MY 认领价差风险事件（observe-only） ----------------

def test_resolve_price_mismatch_emits_claim_anomaly(fake):
    """认领彻底失败且存在同码/同向/同量、仅 WtPrice 不符的委托 → 发
    claim_price_mismatch（含期望价/柜台价/倒计时），认领结果仍为空。"""
    events = []
    d = _connected_dispatcher(on_claim_anomaly=events.append)
    fake.orders = [_order(price=10.6, ref="W901")]
    ref = d._resolve_order_id("000001.SZ", "buy", 10.5, 1000, attempts=1)

    assert ref == ""                       # 不按价差绑单
    assert "W901" not in d._claimed_refs
    assert len(events) == 1
    ev = events[0]
    assert ev["gate"] == "claim_price_mismatch"
    assert ev["rule"] == "claim_price_mismatch"
    assert ev["stock_code"] == "000001.SZ"
    assert ev["direction"] == "buy"
    assert ev["volume"] == 1000
    assert ev["expected_price"] == pytest.approx(10.5)
    assert ev["expire_in_seconds"] == 180
    assert ev["candidates"] == [{
        "order_ref": "W901", "counter_price": 10.6, "diff": 0.1}]
    assert "W901" in ev["message"]


def test_resolve_price_mismatch_compares_by_cent(fake):
    """按分比对：柜台价 10.5001 与审批价 10.5 同分 → 不算价差，正常认领。"""
    events = []
    d = _connected_dispatcher(on_claim_anomaly=events.append)
    fake.orders = [_order(price=10.5001, ref="W902")]
    ref = d._resolve_order_id("000001.SZ", "buy", 10.5, 1000, attempts=1)
    assert ref == "W902"
    assert events == []


def test_resolve_price_match_does_not_emit_claim_anomaly(fake):
    """价符一致正常认领 → 不发价差事件。"""
    events = []
    d = _connected_dispatcher(on_claim_anomaly=events.append)
    fake.orders = [_order(price=10.5, ref="W903")]
    assert d._resolve_order_id("000001.SZ", "buy", 10.5, 1000, attempts=1) == "W903"
    assert events == []


def test_resolve_price_mismatch_without_callback_is_silent(fake):
    """未挂回调（旧调用方）→ 仅 WARNING，不抛异常。"""
    d = _connected_dispatcher()
    fake.orders = [_order(price=10.6, ref="W904")]
    assert d._resolve_order_id("000001.SZ", "buy", 10.5, 1000, attempts=1) == ""


def test_query_deals_side_from_order_bsflag(fake):
    """B8：委托行带 BSFlag 时 query_deals 透出 side，_deal_trade_type 可判方向。"""
    from core.engine.live.order_machine import _deal_trade_type
    fake.orders = [_order(code="601827", flag=1, status=3, filled=5300, ref="W1")]
    d = TdxTqDispatcher(account="123456")
    deal = d.query_deals()[0]
    assert deal["side"] == "sell"
    assert deal["side_source"] == "order_lookup"
    assert _deal_trade_type(deal) == "sell"


def test_query_deals_side_from_contract_lookup(fake):
    """B8：成交行缺方向字段时，按合同编号回查同批委托列表取方向。

    分笔成交共用同一合同编号 → 归回同一委托；这里用「有方向的委托行 +
    无方向的成交行」模拟。
    """
    deal_row = {"Wtbh": "W1", "Code": "688543", "Status": 3, "WtPrice": 30.0,
                "WtVol": 1000, "CjVol": 1000, "CjPrice": 30.0, "Htbh": "H123"}
    order_row = {"Wtbh": "W0", "Code": "688543", "BSFlag": 1, "Status": 1,
                 "WtPrice": 30.0, "WtVol": 1000, "CjVol": 0, "Htbh": "H123"}
    fake.orders = [deal_row, order_row]
    d = TdxTqDispatcher(account="123456")
    deal = d.query_deals()[0]
    assert deal["order_ref"] == "W1"
    assert deal["side"] == "sell"
    assert deal["side_source"] == "order_lookup"


def test_query_deals_side_from_position_delta(fake, caplog):
    """B8：委托/合同编号都查不到方向 → 持仓增量推断（买=增），并打 WARNING。"""
    fake.orders = [{"Wtbh": "W1", "Code": "300174", "Status": 3, "WtPrice": 20.0,
                    "WtVol": 500, "CjVol": 500, "CjPrice": 20.0}]
    fake.positions = [{"Code": "300174", "TotalVol": 1000,
                       "CanUseVol": 1000, "Cbj": 20.0}]
    d = TdxTqDispatcher(account="123456")
    first = d.query_deals()[0]
    assert "side" not in first  # 首次无基线，不凭空推断
    fake.positions = [{"Code": "300174", "TotalVol": 1500,
                       "CanUseVol": 1500, "Cbj": 20.0}]
    with caplog.at_level(logging.WARNING):
        second = d.query_deals()[0]
    assert second["side"] == "buy"
    assert second["side_source"] == "position_delta"
    assert "position delta" in caplog.text


# ------------------------------------------------ 2026-11-18 点击未生效收口


class _StubConfirmer:
    """假确认器：记录 pending_row_state 收到的 spec。"""

    def __init__(self, state="pending"):
        self.state = state
        self.specs = []

    def pending_row_state(self, spec):
        self.specs.append(spec)
        return self.state


def _live_order(**kw):
    base = dict(
        stock_code="301600.SZ", trade_type=TradeType.SELL, quantity=220,
        price=Decimal("64.01"),
        created_at=datetime(2026, 11, 18, 3, 8, 42),  # UTC naive → 11:08:42 上海
    )
    base.update(kw)
    return SimpleNamespace(**base)


def test_last_unconfirmed_message_property():
    """TDX 补齐 last_unconfirmed_message，引擎可把它写进 live_orders.error_message。"""
    d = TdxTqDispatcher()
    assert d.last_unconfirmed_message == ""
    d._last_unconfirmed_message = "已受理未认领：已点击『发送』但行仍待发送"
    assert d.last_unconfirmed_message == "已受理未认领：已点击『发送』但行仍待发送"


def test_pending_row_state_builds_spec_from_live_order():
    stub = _StubConfirmer("pending")
    d = TdxTqDispatcher(auto_confirm_dialog=True, confirmer=stub)
    assert d.pending_row_state(_live_order()) == "pending"
    spec = stub.specs[0]
    assert spec.code6 == "301600"
    assert spec.direction == "sell"
    assert spec.volume == 220
    assert spec.sent_at == datetime(2026, 11, 18, 11, 8, 42)


def test_pending_row_state_disabled_when_auto_confirm_off():
    """自动确认关闭（纯人工盯盘）→ 通道不支持复核，返回 None 保持 expire。"""
    stub = _StubConfirmer("pending")
    d = TdxTqDispatcher(auto_confirm_dialog=False, confirmer=stub)
    assert d.pending_row_state(_live_order()) is None
    assert stub.specs == []


# ---------------- 信号口径 = 不复权（2026-11-22 口径切换） ----------------
def test_query_quote_requests_unadjusted_bars(monkeypatch):
    """query_quote 是信号/执行报价统一入口：必须显式取不复权（真实价）。"""
    idx = [pd.Timestamp("2026-11-20 14:59:00")]
    fq = FakeQuoteData({"1m": {"Close": pd.DataFrame(
        {"603187.SH": [10.22]}, index=idx)}})
    monkeypatch.setattr("core.tq.data.TQData", lambda: fq)
    bars = TdxTqDispatcher().query_quote("603187", period="1m", count=5)
    assert bars and bars[0]["close"] == 10.22
    assert fq.history_calls[0][2]["dividend_type"] == "none"


def test_query_quotes_batch_requests_unadjusted_bars(monkeypatch):
    """批量行情与逐只同口径，不得一条路径前复权、另一条不复权。"""
    idx = [pd.Timestamp("2026-11-20 14:59:00")]
    fq = FakeQuoteData({"1m": {"Close": pd.DataFrame(
        {"603187.SH": [10.22], "600211.SH": [45.0]}, index=idx)}})
    monkeypatch.setattr("core.tq.data.TQData", lambda: fq)
    out = TdxTqDispatcher().query_quotes_batch(
        ["603187", "600211"], period="1m", count=5)
    assert set(out) == {"603187", "600211"}
    assert fq.history_calls[0][2]["dividend_type"] == "none"


def test_daily_close_series_keeps_front_caliber_for_limit_base(monkeypatch):
    """涨跌停前收基准必须前复权（除权日参考价），不得被信号口径改动波及。"""
    idx = [pd.Timestamp("2026-11-19"), pd.Timestamp("2026-11-20")]
    fq = FakeQuoteData({"1d": {"Close": pd.DataFrame(
        {"603187.SH": [10.0, 10.2]}, index=idx)}})
    monkeypatch.setattr("core.tq.data.TQData", lambda: fq)
    series = TdxTqDispatcher()._daily_close_series("603187.SH", 10)
    assert list(series) == [10.0, 10.2]
    assert fq.history_calls[0][2]["dividend_type"] == "front"

