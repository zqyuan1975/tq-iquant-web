"""P0-3（2026-11-24）回归：TDX 裸 ``-1`` 不再吞掉真实失败原因。

真实故障（2026-11-24 09:15）：两笔 BUY 柜台只回 ``-1``，异常文本只有
「通达信下单失败，返回 -1」，无法区分未登录/未就绪/参数被拒；引擎按默认
「可重排」反复撞同一故障。

覆盖：
- ``-1`` + stdout「未登录」→ BridgeUnavailableError（置通道离线）；
- ``-1`` + stdout「可用资金不足」→ BridgeOrderRejected 且被判终态（不重排）；
- ``-1`` 无任何输出 → 维持原 BridgeOrderRejected（回归不变）。
"""
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

import core.engine.tdx_tq_dispatcher as tdx
from core.engine.event import OrderEvent
from core.engine.http_bridge_dispatcher import (
    BridgeOrderRejected, BridgeUnavailableError,
)
from core.engine.live_engine import _is_retryable_reject
from tq_iquant_shared.constants import SignalType, TradeType

_BT = datetime(2026, 11, 24, 9, 15)


def _order(trade_type=TradeType.BUY):
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code="600000.SH",
        trade_type=trade_type, signal_type=SignalType.OPEN,
        quantity=100, price=Decimal("9.30"),
        bar_time=_BT, signal_name="open_sig",
    )


def _dispatcher(monkeypatch, order_stock):
    const = SimpleNamespace(
        STOCK_BUY=0, STOCK_SELL=1,
        PRICE_MY=0, PRICE_SJ=1, PRICE_ZTJ=2, PRICE_DTJ=3,
    )
    monkeypatch.setattr(tdx, "_tqconst", lambda: const)
    disp = tdx.TdxTqDispatcher(auto_confirm_dialog=False)
    disp._account_id = 1
    disp._connected = True
    disp._ensure = lambda: None            # 不真连接客户端
    disp._tq = lambda: SimpleNamespace(order_stock=order_stock)
    return disp


def test_minus_one_login_hint_is_unavailable(monkeypatch):
    def order_stock(**_kw):
        print("下单失败：用户未登录，请先登录交易账户")
        return -1

    disp = _dispatcher(monkeypatch, order_stock)
    with pytest.raises(BridgeUnavailableError) as ei:
        disp.place_order(_order())
    msg = str(ei.value)
    assert "未登录" in msg, "必须回显 stdout 真实原因"
    assert "返回 -1" in msg, "保留原始回执，便于柜台对账"


def test_minus_one_funds_hint_is_terminal_reject(monkeypatch):
    def order_stock(**_kw):
        print("委托失败：可用资金不足")
        return -1

    disp = _dispatcher(monkeypatch, order_stock)
    with pytest.raises(BridgeOrderRejected) as ei:
        disp.place_order(_order())
    msg = str(ei.value)
    assert "可用资金不足" in msg
    assert _is_retryable_reject(ei.value) is False, "资金类拒单必须终态，不能重排"


def test_minus_one_without_output_keeps_reject(monkeypatch):
    def order_stock(**_kw):
        return -1

    disp = _dispatcher(monkeypatch, order_stock)
    with pytest.raises(BridgeOrderRejected):
        disp.place_order(_order())


def test_output_tail_is_per_call(monkeypatch):
    """上一次调用的输出不得串到本次失败信息里。"""
    def first(**_kw):
        print("上一次调用的噪声")
        return 1

    def second(**_kw):
        return -1

    disp = _dispatcher(monkeypatch, first)
    tdx._call(disp._tq().order_stock, account_id=1)
    disp._tq = lambda: SimpleNamespace(order_stock=second)
    with pytest.raises(BridgeOrderRejected) as ei:
        disp.place_order(_order())
    assert "上一次调用的噪声" not in str(ei.value)
