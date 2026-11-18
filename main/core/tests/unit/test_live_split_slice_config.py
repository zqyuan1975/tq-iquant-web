"""H3 切片增强 / H4 自适应追价的纯函数与配置管线测试。"""
from decimal import Decimal

from core.api.strategies import StrategyCreate
from core.engine.live_engine import choose_requote_price, plan_split_count


def test_default_split_count_uses_threshold():
    # 2000 股 × 10 元 = 2 万；阈值 1 万 → 2 笔
    assert plan_split_count(2000, Decimal("20000"), Decimal("10000")) == 2


def test_split_count_override():
    assert plan_split_count(
        1000, Decimal("10000"), Decimal("10000"), count=3) == 3


def test_split_volume_pct_raises_count():
    # bar 量 1000 股、比例 0.2 → 每笔上限 200 股；600 股至少 3 笔
    assert plan_split_count(
        600, Decimal("6000"), Decimal("1000000"),
        volume_pct=0.2, bar_volume=1000) == 3


def test_split_min_qty_caps_count():
    # 1000 股、最小 250 股 → 最多 4 笔（显式想要 10 笔也要被压回 4）
    assert plan_split_count(
        1000, Decimal("10000"), Decimal("100"), count=10,
        min_qty=250) == 4


def test_split_count_never_exceeds_quantity_or_zero():
    assert plan_split_count(
        100, Decimal("1000"), Decimal("1"), count=500) == 100
    assert plan_split_count(0, Decimal("0"), Decimal("1"), count=5) == 1


def test_requote_tick_mode_unchanged():
    assert choose_requote_price(
        Decimal("10.00"), Decimal("0.01"), True, None, 5) == Decimal("10.01")
    assert choose_requote_price(
        Decimal("10.00"), Decimal("0.01"), False, None, 5) == Decimal("9.99")


def test_requote_adaptive_chases_latest_within_cap():
    # 买：最新 10.05 → 10.06；上限 orig+5 档 = 10.05 → 夹到 10.05
    assert choose_requote_price(
        Decimal("10.00"), Decimal("0.01"), True, Decimal("10.05"), 5) == Decimal("10.05")
    # 买：最新 9.90（低于原价）→ 至少追一档 10.01
    assert choose_requote_price(
        Decimal("10.00"), Decimal("0.01"), True, Decimal("9.90"), 5) == Decimal("10.01")


def test_requote_adaptive_sell_and_zero_cap_fallback():
    assert choose_requote_price(
        Decimal("10.00"), Decimal("0.01"), False, Decimal("9.95"), 5) == Decimal("9.95")
    # max_ticks<=0 退回 tick 行为
    assert choose_requote_price(
        Decimal("10.00"), Decimal("0.01"), True, Decimal("10.50"), 0) == Decimal("10.01")


def test_strategy_create_accepts_h3_h4_fields():
    req = StrategyCreate(
        name="s", pool_id=1, period="1d", formula_id=1, role="master",
        split_order_enabled=True, split_order_count=4, split_order_min_qty=200,
        split_order_interval_seconds=3.0, split_order_interval_jitter=True,
        split_order_volume_pct=0.2, requote_price_mode="adaptive",
        requote_max_slippage_ticks=3,
    )
    assert req.split_order_count == 4
    assert req.requote_price_mode == "adaptive"
