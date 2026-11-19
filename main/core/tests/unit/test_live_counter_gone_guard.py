"""counter_gone 误判加固单测（2026-11-13）。

事故：通达信交易引擎未就绪时 ``query_stock_positions`` 返回空表（不置 last_error）、
``query_stock_asset`` 报连接失败 → 采集把「空表」当成「人工清仓」，连续 2 次确认后
清掉 3 只虚拟仓并按成本回补现金，随后 09:15 卖单被 T+1 全拦。

加固口径：
1. 持仓空表 + 资产口径不可信（total_asset 缺失 / 与 available 不符 / 账户市值 > 0）
   → 判**读取失败**：不写空快照、不累计清仓确认，只对账内虚拟仓发 notice；
2. 真正清仓（资产可读、账户市值 0）仍按连续 2 次确认清理并留 apply 审计；
3. 部分代码缺失但账户读取可信时，人工卖出检测保持原行为（不被本次加固削弱）。
"""
from datetime import datetime
from decimal import Decimal
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.bar_poller import BarPoller
from core.engine.live_engine import LiveEngine
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.models import (
    Base, LiveAccountSnapshot, LiveDecisionEvent,
)
from tq_iquant_shared.constants import SignalType

_STOCK = "002922.SZ"
_OTHER = "300286.SZ"
_SID = 1
_STRATEGY = 1


def _db_factory():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    def factory():
        return SessionLocal()

    return factory, engine


class _StubDispatcher:
    """最小 dispatcher：柜台持仓可配置；资产端点缺省时模拟「查询不可用」。"""

    channel = "tdx_tq"

    def __init__(self, positions=None, asset=None):
        self._positions = positions if positions is not None else []
        self._asset = asset

    def get_positions(self):
        return list(self._positions)

    def get_asset(self):
        if self._asset is None:
            raise RuntimeError("与通达信客户端建立连接失败")
        return dict(self._asset)

    def query_positions(self):
        return []

    def get_cash(self):
        return None

    def query_quote(self, code, period="1m", count=10):
        return []

    def query_calendar(self, *args, **kwargs):
        raise RuntimeError("calendar unavailable")

    def execution_receipts(self):
        return []

    def check_price_limit(self, *args, **kwargs):
        return None

    def price_limit_bounds(self, *args, **kwargs):
        return None


def _portfolio(initial=Decimal("500000")):
    pm = PortfolioRiskManager(
        max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=_SID, initial_capital=initial, risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=_STRATEGY, period="1d",
        capital_ratio=Decimal("0.6"), max_positions=5,
        single_open_ratio=Decimal("0.1"),
    )
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN,
         "trigger_value": 1},
    ]
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"),
        take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0.03"),
    )
    port.strategies.append(ctx)
    return port


def _with_position(port, code=_STOCK, qty=2500, cost="22.00"):
    pos = Position(code)
    pos.buy(qty, Decimal(cost), datetime(2026, 11, 10, 9, 31, 54))
    port.strategies[0].positions[code] = pos
    return pos


def _engine(factory, disp, port):
    poller = BarPoller(disp, [_STOCK], period="1m", count=10)
    return LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )


def _events(db, gate="manual_intervention"):
    return db.query(LiveDecisionEvent).filter(
        LiveDecisionEvent.gate == gate).all()


def _position_qty(port, code=_STOCK):
    pos = port.strategies[0].positions.get(code)
    return 0 if pos is None else pos.quantity


# ---------------- 1. 空表 + 资产读取失败 → 跳过本轮，绝不清仓 ----------------
def test_empty_positions_with_failed_asset_read_never_clears():
    factory, _ = _db_factory()
    port = _portfolio()
    _with_position(port)
    cash_before = port.account.cash
    engine = _engine(factory, _StubDispatcher(positions=[], asset=None), port)
    db = factory()

    engine._capture_account_snapshot(db)
    engine._capture_account_snapshot(db)
    db.commit()
    events = _events(db)
    snapshots = db.query(LiveAccountSnapshot).count()
    db.close()

    assert _position_qty(port) == 2500            # 虚拟仓未被清
    assert port.account.cash == cash_before        # 现金未被回补
    assert snapshots == 0                          # 不写空快照
    assert [e.action for e in events] == ["notice"]
    assert "未清仓" in events[0].message


# ---------------- 2. 空表 + 资产口径自相矛盾 → 同样只 notice ----------------
def test_empty_positions_with_inconsistent_asset_only_notices():
    factory, _ = _db_factory()
    port = _portfolio()
    _with_position(port)
    engine = _engine(factory, _StubDispatcher(
        positions=[],
        asset={"total_asset": Decimal("270872.51"),
               "available": Decimal("108513.51"),
               "market_value": Decimal("162359.00")},
    ), port)
    db = factory()

    engine._capture_account_snapshot(db)
    engine._capture_account_snapshot(db)
    db.commit()
    events = _events(db)
    snapshots = db.query(LiveAccountSnapshot).count()
    db.close()

    assert _position_qty(port) == 2500
    assert snapshots == 0
    assert [e.action for e in events] == ["notice"]


# ---------------- 5. 不可信读取的跳过日志：连续只告警一次，恢复后重置 ----------------
def test_repeated_untrusted_skip_warning_is_throttled(caplog):
    """终端关闭/未就绪导致的持续跳过不得每 5s 刷 WARNING（2026-11-18 实证）。"""
    factory, _ = _db_factory()
    port = _portfolio()
    _with_position(port)
    disp = _StubDispatcher(positions=[], asset=None)
    engine = _engine(factory, disp, port)
    db = factory()

    with caplog.at_level(logging.DEBUG, logger="core.engine.live_engine"):
        engine._capture_account_snapshot(db)
        engine._capture_account_snapshot(db)
        engine._capture_account_snapshot(db)
        skips = [
            r for r in caplog.records
            if "account snapshot skipped" in r.getMessage()
        ]
        assert [r.levelno for r in skips] == [
            logging.WARNING, logging.DEBUG, logging.DEBUG]

        # 账户恢复可读（真实持仓 + 资产口径一致）→ 成功写快照并重置节流
        disp._positions = [{"symbol": _STOCK, "volume": 2500, "available": 2500,
                            "cost": "22.00"}]
        disp._asset = {"total_asset": Decimal("555000"),
                       "available": Decimal("505000"),
                       "market_value": Decimal("50000")}
        caplog.clear()
        engine._capture_account_snapshot(db)
        assert not [r for r in caplog.records
                    if "account snapshot skipped" in r.getMessage()]

        # 再次进入不可信状态 → 重新给出首条 WARNING
        disp._positions = []
        disp._asset = None
        caplog.clear()
        engine._capture_account_snapshot(db)
        skips = [
            r for r in caplog.records
            if "account snapshot skipped" in r.getMessage()
        ]
        assert [r.levelno for r in skips] == [logging.WARNING]

    db.close()


# ---------------- 3. 真实清仓（资产可读、市值 0）仍按 2 次确认清仓 ----------------
def test_genuine_liquidation_still_clears_after_two_confirmations():
    factory, _ = _db_factory()
    port = _portfolio()
    _with_position(port)
    cash_before = port.account.cash
    engine = _engine(factory, _StubDispatcher(
        positions=[],
        asset={"total_asset": Decimal("108513.51"),
               "available": Decimal("108513.51"),
               "market_value": Decimal("0")},
    ), port)
    db = factory()

    engine._capture_account_snapshot(db)   # 第 1 次：仅累计确认
    db.commit()
    assert _position_qty(port) == 2500

    engine._capture_account_snapshot(db)   # 第 2 次：确认清仓
    db.commit()
    events = _events(db)
    db.close()

    assert _position_qty(port) == 0
    assert port.account.cash == cash_before + Decimal("55000.00")
    assert [e.action for e in events] == ["apply"]
    assert "柜台已无此持仓" in events[0].message


# ---------------- 4. 部分代码缺失 + 账户可信 → 人工卖出检测保持原行为 ----------------
def test_partial_missing_with_trusted_account_still_clears():
    factory, _ = _db_factory()
    port = _portfolio()
    _with_position(port)
    _with_position(port, code=_OTHER, qty=2900, cost="18.403")
    engine = _engine(factory, _StubDispatcher(
        positions=[{"symbol": _OTHER, "volume": 2900, "available": 2900,
                    "cost": "18.403"}],
        asset={"total_asset": Decimal("160000"),
               "available": Decimal("100000"),
               "market_value": Decimal("60000")},
    ), port)
    db = factory()

    engine._capture_account_snapshot(db)
    engine._capture_account_snapshot(db)
    db.commit()
    events = [e for e in _events(db) if e.stock_code == _STOCK]
    db.close()

    assert _position_qty(port, _STOCK) == 0        # 缺失代码按人工卖出清理
    assert _position_qty(port, _OTHER) == 2900     # 仍在的代码不动
    assert [e.action for e in events] == ["apply"]
