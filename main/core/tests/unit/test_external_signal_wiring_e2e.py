"""外部信号文件链路端到端（接线验证）：真实 ExternalSignalFileSource + 真实引擎
闸门链，只把券商通道换成 HttpBridgeDispatcher 的 MockTransport。

覆盖：真实通达信格式落盘 → 5s 节拍读取 → 公式名匹配 → 归属过滤 → 注入
signal_cache → _handle_bar 全套风控 → 委托落库；同分钟重复行、池外代码、末段
半行、启动前历史行四类边界。
"""
import logging
from datetime import datetime, timedelta
from decimal import Decimal

import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.bar_poller import BarPoller
from core.engine.http_bridge_dispatcher import HttpBridgeDispatcher
from core.engine.live_engine import LiveEngine, now_shanghai
from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.models import Base, LiveOrder
from tq_iquant_shared.constants import SignalType


def _db_factory():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    return SessionLocal, engine


class _Recorder:
    def __init__(self):
        self.requests = []

    def handler(self, request):
        self.requests.append(request)
        path = request.url.path
        if path == "/order":
            return httpx.Response(200, json={"ok": True})
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if path == "/positions":
            return httpx.Response(200, json={"ok": True, "data": []})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})


def _portfolio(period="1m", pool=None):
    pm = PortfolioRiskManager(
        max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    port = Portfolio(
        portfolio_id=1, initial_capital=Decimal("100000"), risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=1, period=period,
        capital_ratio=Decimal("0.6"), max_positions=5,
        single_open_ratio=Decimal("0.1"),
    )
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN,
         "trigger_value": 1},
    ]
    ctx.formula_name = "分时突击"
    ctx.stock_pool = pool
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"),
        take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx)
    return port, ctx


def _append_tdx_row(path, code, name, minute, price, pct, volume, formula):
    """按通达信写入方式追加一行：GBK + CRLF + Tab 分隔 + 行尾多余 Tab。"""
    line = "\t".join([code, name, minute, price, pct, volume, formula]) + "\t\r\n"
    with open(path, "ab") as f:
        f.write(line.encode("gbk"))


def _orders(factory):
    db = factory()
    try:
        return db.query(LiveOrder).order_by(LiveOrder.id).all()
    finally:
        db.close()


def test_external_signal_file_end_to_end(tmp_path, caplog):
    factory, _ = _db_factory()
    port, ctx = _portfolio(pool={"600000.SH"})
    rec = _Recorder()
    disp = HttpBridgeDispatcher(
        base_url="http://127.0.0.1:8790",
        client=httpx.Client(transport=httpx.MockTransport(rec.handler)))
    path = tmp_path / "BDXH.txt"
    # 通达信启动补齐的历史行（早于启动 → 游标挡住，不交易）
    _append_tdx_row(path, "300456", "赛微电子", "2026-09-08 23:08",
                    "37.61", "-2.77%", "5387", "分时突击")
    with caplog.at_level(logging.INFO):
        eng = LiveEngine(
            session_id=1, portfolios=[port], dispatcher=disp,
            bar_poller=BarPoller(disp, ["600000.SH"], period="1m", count=10),
            db_session_factory=factory, signal_file_path=str(path),
            formula_by_strategy={1: "分时突击"})
        st = eng.external_signal_status()
        assert st["enabled"] is True
        assert st["path_exists"] is True
        assert st["formula_names"] == ["分时突击"]
        assert "external signal source enabled" in caplog.text
        # 测试固定信号时钟到交易日盘中（真实运行时游标=启动时刻，启动所在那一
        # 分钟的历史行不消费、宁漏不重；固定时钟只为让「新增行」可控可测）。
        fixed_now = datetime(2026, 11, 20, 9, 50)
        eng._signal_source._clock = lambda: fixed_now
        eng._signal_source._last_read_time = datetime(2026, 11, 20, 9, 45)

        # 无新行：不产生委托
        eng._tick_external_signals()
        assert _orders(factory) == []

        minute = "2026-11-20 09:50"
        _append_tdx_row(path, "600000", "浦发银行", minute,
                        "9.30", "1.00%", "600", "分时突击")
        eng._tick_external_signals()
        orders = _orders(factory)
        assert len(orders) == 1
        assert orders[0].stock_code == "600000.SH"
        assert orders[0].trade_type == "buy"
        # 文件时间戳作为 bar_time（非墙钟）
        assert orders[0].bar_time == datetime.strptime(minute, "%Y-%m-%d %H:%M")
        assert "external signal inject:" in caplog.text

        # 同分钟晚到第二行（不同代码、同池）= 被消费但不重复下单同代码同方向；
        # 且源层有 same-minute 留痕
        _append_tdx_row(path, "600000", "浦发银行", minute,
                        "9.31", "1.10%", "100", "分时突击")
        eng._tick_external_signals()
        assert len(_orders(factory)) == 1
        assert "same-minute row consumed" in caplog.text
        assert "already consumed" in caplog.text

        # 池外代码：不产生委托，留 out of scope 痕迹
        _append_tdx_row(path, "300456", "赛微电子", minute,
                        "37.61", "-2.77%", "5387", "分时突击")
        eng._tick_external_signals()
        assert len(_orders(factory)) == 1
        assert "out of scope" in caplog.text

        # 末段半行（无换行）不消费；补全后下一拍才消费
        with open(path, "ab") as f:
            f.write("002001\t新和成\t%s\t18.00\t0.50%%\t100\t分时突击\t"
                    .replace("%s", minute).encode("gbk"))
        eng._tick_external_signals()
        assert eng.external_signal_status()["cursor_time"] == minute
        with open(path, "ab") as f:
            f.write(b"\r\n")
        # 002001 不在池内 → 消费但被归属过滤（out of scope 计数增长）
        before = eng.external_signal_status()["consumed_count"]
        eng._tick_external_signals()
        assert eng.external_signal_status()["consumed_count"] == before + 1
        assert len(_orders(factory)) == 1

        st = eng.external_signal_status()
        assert st["consumed_count"] >= 3
        assert st["last_consumed_at"] is not None
        assert st["daily_dedup"] is True
        assert st["poll_interval_seconds"] == 5.0

    # 默认关闭（未配置路径）：链路完全不变
    port2, _ = _portfolio()
    eng2 = LiveEngine(
        session_id=2, portfolios=[port2], dispatcher=disp,
        bar_poller=BarPoller(disp, ["600000.SH"], period="1m", count=10),
        db_session_factory=factory)
    assert eng2.external_signal_status() == {"enabled": False}
    assert eng2._signal_source is None


def test_external_signal_missing_path_logs_error_and_recovers(tmp_path, caplog):
    """路径不存在：启动明确报错（不静默失效），文件出现后恢复读取。"""
    factory, _ = _db_factory()
    port, _ = _portfolio()
    rec = _Recorder()
    disp = HttpBridgeDispatcher(
        base_url="http://127.0.0.1:8790",
        client=httpx.Client(transport=httpx.MockTransport(rec.handler)))
    path = tmp_path / "not_yet.txt"
    with caplog.at_level(logging.ERROR):
        eng = LiveEngine(
            session_id=1, portfolios=[port], dispatcher=disp,
            bar_poller=BarPoller(disp, ["600000.SH"], period="1m", count=10),
            db_session_factory=factory, signal_file_path=str(path),
            formula_by_strategy={1: "分时突击"})
    assert "external signal file not found at startup" in caplog.text
    assert eng.external_signal_status()["path_exists"] is False
    # 首拍文件仍不存在：源层留一条缺失告警（只告警一次）
    with caplog.at_level(logging.WARNING):
        eng._tick_external_signals()
    assert "外部信号文件不存在" in caplog.text
    # 文件出现后：下一拍恢复并出现恢复日志
    _append_tdx_row(path, "300456", "赛微电子", "2026-09-08 23:08",
                    "37.61", "-2.77%", "5387", "分时突击")
    with caplog.at_level(logging.INFO):
        eng._tick_external_signals()
    assert "外部信号文件已出现，恢复读取" in caplog.text
    assert eng.external_signal_status()["path_exists"] is True
