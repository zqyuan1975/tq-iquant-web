"""15:05 日线轮次（DailyCloser.maybe_daily_bars）丢轮修复回归（2026-11-16）。

P1：旧实现**驱动前**就写 daily_bars_done_date 并 commit，而驱动循环遇
BridgeUnavailableError 直接 return → 多组合会话一次短暂掉线（或进程在第一个
组合驱动后崩溃）即留下「已标记完成但剩余组合/周期从未驱动」，当天永久不再重试。
现行为：先驱动、后标记；离线组合跳过继续驱动其余组合；只要本轮有组合未驱动就
不标完成，由下一轮 60s tick 重试，且重试不重复驱动已成功的 (周期, 组合)。
"""
from datetime import date, datetime
from types import SimpleNamespace

from core.engine.http_bridge_dispatcher import BridgeUnavailableError
from core.engine.live.daily_closer import DailyCloser
from core.tests.unit.test_live_engine import _db_factory

_TODAY = date(2026, 8, 5)  # 周三（非周五/月末 → 只驱动 1d）
_NOW = datetime(2026, 8, 5, 15, 6)


class _Cal:
    def is_trading_day(self, d):
        return True

    def is_last_trading_day_of_week(self, d):
        return False

    def is_last_trading_day_of_month(self, d):
        return False


class _Disp:
    """1d 快照：最后一根 stime == 今日（完成性校验通过）。"""

    def __init__(self):
        self.bars = [{
            "stime": "20260805", "open": "9.0", "high": "9.3",
            "low": "8.9", "close": "9.1", "volume": "100000",
        }]

    def query_quote(self, code, period="1d", count=250):
        return self.bars


class _MD:
    _strategy_periods = {"1d"}
    _period_count = {"1d": 10}
    _formula_count = 10
    _bar_poller = SimpleNamespace(stock_codes=["600000.SH"])

    def _sort_and_cap(self, bars, count):
        return bars

    def startup_periods_missing(self, daily_time):
        return False

    def inject_startup_periods(self, daily_time):
        return None


def _mk_closer(on_bar):
    factory, _ = _db_factory()
    ctx = SimpleNamespace(
        session_id=1,
        portfolios=[SimpleNamespace(portfolio_id=1), SimpleNamespace(portfolio_id=2)],
        db_session_factory=factory,
        dispatcher=_Disp(),
        code_period_count={},
        clock=lambda: _NOW,
    )
    offline = []
    closer = DailyCloser(
        ctx=ctx,
        market_data=_MD(),
        breaker=None,
        orders=None,
        on_bar=on_bar,
        calendar_provider=lambda: _Cal(),
        emit=lambda *a, **k: None,
        set_bridge_offline=lambda: offline.append(1),
    )
    return closer, offline


def test_bridge_offline_during_drive_does_not_mark_done_and_skips_only_that_portfolio():
    """组合 1 掉线：组合 2 照常驱动；本轮不标完成（不留「已标记未驱动」）。"""
    driven = []
    fail = {"on": True}

    def on_bar(portfolio, bar_event, **kw):
        if portfolio.portfolio_id == 1 and fail["on"]:
            raise BridgeUnavailableError("bridge down")
        driven.append((portfolio.portfolio_id, bar_event.period))

    closer, offline = _mk_closer(on_bar)
    closer.maybe_daily_bars(_NOW)

    assert driven == [(2, "1d")]
    assert offline == [1]
    assert closer.last_daily_bar_date is None
    assert closer._daily_bars_done(_TODAY) is False


def test_retry_drives_only_undriven_portfolio_then_marks_done():
    """下一轮 tick 只补驱动上次掉线的组合 1，不重复驱动组合 2；齐了才标完成。"""
    driven = []
    fail = {"on": True}

    def on_bar(portfolio, bar_event, **kw):
        if portfolio.portfolio_id == 1 and fail["on"]:
            raise BridgeUnavailableError("bridge down")
        driven.append((portfolio.portfolio_id, bar_event.period))

    closer, _ = _mk_closer(on_bar)
    closer.maybe_daily_bars(_NOW)
    assert driven == [(2, "1d")]

    fail["on"] = False
    closer.maybe_daily_bars(datetime(2026, 8, 5, 15, 7))

    assert driven == [(2, "1d"), (1, "1d")]  # 组合 2 未重复驱动
    assert closer.last_daily_bar_date == _TODAY


def test_all_portfolios_driven_marks_done():
    """整轮驱动成功 → 标完成（幂等标记在驱动之后）。"""
    driven = []

    def on_bar(portfolio, bar_event, **kw):
        driven.append((portfolio.portfolio_id, bar_event.period))

    closer, offline = _mk_closer(on_bar)
    closer.maybe_daily_bars(_NOW)

    assert sorted(driven) == [(1, "1d"), (2, "1d")]
    assert offline == []
    assert closer._daily_bars_done(_TODAY) is True


def test_stop_during_drive_does_not_mark_done_and_stops_remaining():
    """停止请求在驱动途中到达：立即收尾，不驱动剩余组合，也不标完成。

    2026-11-19 实证：15:06:22 停止会话，_handle_bar 因停止闸直接短路（信号/风控
    都没跑），但旧实现仍把 (周期,组合) 记为已驱动并在收尾标 daily_bars_done_date，
    账面看起来「日终轮跑过了」。
    """
    driven = []
    state = {"stop": False}

    def on_bar(portfolio, bar_event, **kw):
        driven.append(portfolio.portfolio_id)
        state["stop"] = True  # 模拟驱动途中收到停止请求

    closer, offline = _mk_closer(on_bar)
    closer._ctx.stop_requested = lambda: state["stop"]
    closer.maybe_daily_bars(_NOW)

    assert driven == [1]  # 停止后不再驱动组合 2
    assert offline == []
    assert closer.last_daily_bar_date is None
    assert closer._daily_bars_done(_TODAY) is False


def test_stop_after_last_portfolio_still_does_not_mark_done():
    """最后一个组合刚驱动完就收到停止：收尾前的二次确认同样不能标完成。"""
    driven = []
    state = {"stop": False}

    def on_bar(portfolio, bar_event, **kw):
        driven.append(portfolio.portfolio_id)
        state["stop"] = portfolio.portfolio_id == 2

    closer, _ = _mk_closer(on_bar)
    closer._ctx.stop_requested = lambda: state["stop"]
    closer.maybe_daily_bars(_NOW)

    assert driven == [1, 2]
    assert closer.last_daily_bar_date is None
    assert closer._daily_bars_done(_TODAY) is False


def test_fetch_1d_bars_aborts_on_stop_request():
    """停止中必须中断全池日线拉取：否则 engine.stop() 排队等全池遍历，
    会话永远停在 stopping（掘金桥单只 ~8s，496 只可达数十分钟）。"""
    closer, _ = _mk_closer(lambda *a, **k: None)
    calls = []

    class _CountingDisp(_Disp):
        def query_quote(self, code, period="1d", count=250):
            calls.append(code)
            return self.bars

    closer._market_data._bar_poller = SimpleNamespace(
        stock_codes=["600000.SH", "000001.SZ", "600519.SH"])
    closer._ctx.dispatcher = _CountingDisp()
    closer._ctx.stop_requested = lambda: len(calls) >= 1

    out = closer._fetch_1d_bars()

    assert calls == ["600000.SH"]  # 第一只后立即收尾，不再遍历全池
    assert list(out) == ["600000.SH"]
