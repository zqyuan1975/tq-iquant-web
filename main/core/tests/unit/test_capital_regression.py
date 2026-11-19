"""资金池 B7：默认路径回归基线（开关关闭时逐位一致）。

口径（0021 阶段 B7）：固定 #4（自适应滑点）与一个实盘回放样本，做关闭开关的
黄金对比。覆盖两种配置：

1. 「开关关闭 + 旧配置」：不注入任何资金池字段，等价改动前代码；
2. 「开关关闭 + 新字段存在」：组合带上 `target_utilization`、`slippage_impact_coef`、
   `volume_limit_*` 等新字段，但开关全部关闭（`adaptive_slippage_enabled=False`、
   `volume_limit_enabled=False`、`capital_budget=None`、`loan_model_plan=None`）。

两种配置下「日终快照 + 成交 + 现金」的规范化指纹必须逐位一致；同一份回放样本
的指纹再固定成黄金常量，任何无意改变默认路径的提交都会在这里失败。
"""
import hashlib
import json
from datetime import datetime, timedelta
from decimal import Decimal

import polars as pl

from core.engine.backtest_engine import BacktestEngine
from core.engine.event import SignalType
from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext


STOCK = "000001.SZ"
BARS = 40


def _klines(stock_code, rows):
    df = pl.DataFrame({
        "datetime": [r[0] for r in rows],
        "Open": [r[1] for r in rows],
        "High": [r[2] for r in rows],
        "Low": [r[3] for r in rows],
        "Close": [r[4] for r in rows],
        "Volume": [r[5] for r in rows],
    })
    return {stock_code: {"1d": df}}


def _replay_rows(days=BARS):
    """固定回放样本：40 根日线，价格按 i%7 的确定性锯齿，量逐根递增。

    全部用 Decimal 字面量，无随机、无时钟，CI 可重复。
    """
    rows = []
    prev_close = Decimal("10.00")
    for i in range(days):
        close = Decimal("10.00") + Decimal(i % 7 - 3) / Decimal("10")
        open_ = prev_close
        high = max(open_, close) + Decimal("0.20")
        low = min(open_, close) - Decimal("0.20")
        volume = 100000 + i * 1000
        rows.append((datetime(2026, 3, 2) + timedelta(days=i),
                     open_, high, low, close, volume))
        prev_close = close
    return rows


def _signal_cache(days=BARS):
    """开盘日（i%10==0）触发 OPEN，第五日（i%10==5）触发 CLOSE。"""
    cache = {}
    for i in range(days):
        d = datetime(2026, 3, 2) + timedelta(days=i)
        open_val = 1 if i % 10 == 0 else -1
        close_val = 1 if i % 10 == 5 else -1
        cache[(1, STOCK, d)] = [
            {"name": "open_sig", "value": open_val},
            {"name": "close_sig", "value": close_val},
        ]
    return cache


def _portfolio(*, with_new_fields: bool):
    """构造基准组合。

    with_new_fields=True 时注入资金池/滑点/流动性新字段，但开关保持关闭——
    模拟「迁移已在真实库执行、新列存在，但能力未启用」的线上状态。
    """
    pm = PortfolioRiskManager(
        max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    port = Portfolio(
        portfolio_id=1, initial_capital=Decimal("1000000"), risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=1, period="1d",
        capital_ratio=Decimal("0.5"), max_positions=1,
    )
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN,
         "trigger_value": 1},
        {"signal_name": "close_sig", "signal_type": SignalType.CLOSE,
         "trigger_value": 1},
    ]
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.50"),
        take_profit_ratio=Decimal("0.50"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx)

    if with_new_fields:
        # 0021 A 阶段新增：组合级目标利用率（只被额度推导读取）
        port.target_utilization = Decimal("0.95")
        # #4 自适应滑点系数存在，但 adaptive_slippage_enabled=False
        port.adaptive_slippage_enabled = False
        port.slippage_impact_coef = Decimal("0.25")
        # #2 流动性字段存在，但 volume_limit_enabled=False
        port.volume_limit_enabled = False
        port.volume_limit_ratio = Decimal("0.05")
        # 资金池额度未注入 = B2 默认路径
        ctx.capital_budget = None
    return port, ctx


def _num(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _fingerprint(result, portfolio):
    """规范化「快照 + 成交 + 现金」指纹（sha256）。"""
    payload = {
        "snapshots": [
            {
                "snap_date": _num(s.get("snap_date")),
                "total_value": _num(s.get("total_value")),
                "cash": _num(s.get("cash")),
                "market_value": _num(s.get("market_value")),
                "positions": [
                    {k: _num(v) for k, v in p.items()} for p in s.get("positions") or []
                ],
                "snapshot_keys": sorted(s.keys()),
            }
            for s in result["snapshots"]
        ],
        "trades": [
            {
                "strategy_id": t.strategy_id,
                "stock_code": t.stock_code,
                "trade_type": str(t.trade_type),
                "signal_type": str(t.signal_type),
                "price": _num(t.price),
                "quantity": _num(t.quantity),
                "amount": _num(t.amount),
                "commission": _num(t.commission),
                "stamp_duty": _num(t.stamp_duty),
                "transfer_fee": _num(t.transfer_fee),
                "ref_price": _num(t.ref_price),
                "trade_time": _num(t.trade_time),
            }
            for t in result["trades"]
        ],
        "final_cash": _num(portfolio.account.cash),
        "loan_model": result.get("loan_model"),
    }
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                      default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _run(*, with_new_fields: bool, loan_model_plan=None):
    port, _ctx = _portfolio(with_new_fields=with_new_fields)
    result = BacktestEngine().run(
        port,
        klines=_klines(STOCK, _replay_rows()),
        signal_cache=_signal_cache(),
        loan_model_plan=loan_model_plan,
    )
    return result, port


# 黄金常量：关闭开关时固定回放样本的默认路径指纹（2026-11-20 B7 首次固化）。
GOLDEN_FINGERPRINT = (
    "f081b1e8b2f0c0f3ae6de7f1a52e0634dbe78aa9b2066b4d3233c0474ae86dd9")


def test_switches_off_legacy_vs_new_fields_bit_identical():
    """开关关闭时，新字段存在与否的「快照/成交/现金」指纹必须一致。"""
    legacy_result, legacy_port = _run(with_new_fields=False)
    new_result, new_port = _run(
        with_new_fields=True, loan_model_plan=None)

    assert legacy_result["trades"], "回放样本应至少产生一笔成交"
    assert _fingerprint(legacy_result, legacy_port) == _fingerprint(
        new_result, new_port)

    # 新字段存在但开关关闭：不得写资金池快照键，也不得返回借据模型
    for snap in new_result["snapshots"]:
        assert "loan_model_version" not in snap
        assert "capital_pool_version" not in snap
        assert "entitlement_json" not in snap
    assert new_result["loan_model"] is None


def test_issue4_adaptive_slippage_off_matches_legacy():
    """#4 自适应滑点：系数存在但开关关闭，成交价与成交额须与旧配置逐位一致。"""
    legacy_result, _ = _run(with_new_fields=False)
    new_result, _ = _run(with_new_fields=True)

    assert [(str(t.trade_type), t.price, t.quantity, t.amount)
            for t in legacy_result["trades"]] == \
           [(str(t.trade_type), t.price, t.quantity, t.amount)
            for t in new_result["trades"]]

    # 同样的流动性字段存在但 volume_limit_enabled=False：成交量不被缩减
    legacy_qty = sum(t.quantity for t in legacy_result["trades"])
    new_qty = sum(t.quantity for t in new_result["trades"])
    assert legacy_qty == new_qty


def test_live_replay_sample_golden_fingerprint():
    """实盘回放样本的默认路径指纹固定为黄金常量，防止默认行为被无意改变。"""
    result, port = _run(with_new_fields=False)
    assert _fingerprint(result, port) == GOLDEN_FINGERPRINT
