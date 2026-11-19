"""M2：柜台不可核（untraceable）的委托不占在途资金预留（A3，2026-11-16）。

可核的 submitted/partial 仍按「挂单价 × 未成交量」占用，绝不放宽真实在途单。
"""
import threading
from decimal import Decimal
from types import SimpleNamespace

from core.engine.live_engine import LiveEngine
from core.engine.live.order_machine import _UNTRACEABLE_MARKER


def _order(**overrides):
    base = dict(
        trade_type="buy", status="submitted", portfolio_strategy_id=1,
        quantity=1000, filled_quantity=0, price=Decimal("10.00"),
        error_message=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _engine(*orders):
    eng = object.__new__(LiveEngine)
    eng._book_lock = threading.RLock()
    eng._order_machine = SimpleNamespace(
        pending_orders={idx: row for idx, row in enumerate(orders)})
    return eng


def test_untraceable_order_not_reserved():
    eng = _engine(_order(error_message=_UNTRACEABLE_MARKER + "柜台查询超时"))
    assert eng._reserved_buy_cash() == Decimal("0")


def test_traceable_inflight_still_reserved():
    eng = _engine(
        _order(),                                  # 1000 × 10 = 10000
        _order(quantity=500, filled_quantity=200),  # 300 × 10 = 3000
    )
    assert eng._reserved_buy_cash() == Decimal("13000.00")


def test_mixed_untraceable_and_traceable():
    eng = _engine(
        _order(error_message=_UNTRACEABLE_MARKER + "无法核验"),
        _order(quantity=100, price=Decimal("8.00")),
    )
    assert eng._reserved_buy_cash() == Decimal("800.00")


def test_sell_orders_never_reserved():
    eng = _engine(_order(trade_type="sell"))
    assert eng._reserved_buy_cash() == Decimal("0")
