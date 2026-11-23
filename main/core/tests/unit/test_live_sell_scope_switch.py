"""会话级卖出范围开关（存量/外部仓是否代卖）聚焦用例。

覆盖：
- 默认关闭（全局缺键/非法 → platform_only）；
- 解析优先级（会话列 > 全局）；
- 引擎每轮 tick 重读会话列（下一轮生效、可切回）；
- 读库异常保持当前档位（fail-safe，不扩大代卖范围）；
- 切换只改卖出数量口径：不撤单、不改持仓数量；
- 切换服务立即通知运行中引擎、且不反向改写全局配置。
"""
from datetime import datetime
from types import SimpleNamespace

from core.services import live_service as svc
from core.engine.live_engine import LiveEngine


# ---------------------------------------------------------------------------
# fakes
# ---------------------------------------------------------------------------
class _Row:
    def __init__(self, scope):
        self.sell_scope = scope


class _FakeDB:
    """极简 Session 替身：query().filter().first() + commit/close。"""

    def __init__(self, row=None, exc=None):
        self._row, self._exc = row, exc
        self.committed = False
        self.closed = False

    def query(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def first(self):
        if self._exc is not None:
            raise self._exc
        return self._row

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


def _ports(*scopes):
    return [SimpleNamespace(sell_scope=s) for s in scopes]


def _engine(scopes=("platform_only",), row=None, exc=None,
            global_scope="platform_only"):
    eng = SimpleNamespace(
        session_id=7,
        _sell_scope=scopes[0],
        ctx=SimpleNamespace(portfolios=_ports(*scopes)),
        _db_session_factory=lambda: _FakeDB(row, exc),
        _global_sell_scope=lambda: global_scope,
    )
    eng.apply_sell_scope = lambda value: LiveEngine.apply_sell_scope(eng, value)
    return eng


# ---------------------------------------------------------------------------
# 引擎侧：tick 刷新 + 即时应用
# ---------------------------------------------------------------------------
def test_apply_sell_scope_syncs_all_portfolios_and_ignores_invalid():
    eng = _engine(("platform_only", "platform_only"))
    LiveEngine.apply_sell_scope(eng, "full")
    assert eng._sell_scope == "full"
    assert [p.sell_scope for p in eng.ctx.portfolios] == ["full", "full"]
    # 非法值忽略，不改变现有档位
    LiveEngine.apply_sell_scope(eng, "whatever")
    assert eng._sell_scope == "full"


def test_refresh_sell_scope_takes_effect_next_tick_and_can_toggle_back():
    eng = _engine(row=_Row("full"), global_scope="platform_only")
    LiveEngine._refresh_sell_scope(eng)
    assert eng._sell_scope == "full"
    assert eng.ctx.portfolios[0].sell_scope == "full"
    # 切回：会话列写回 platform_only → 下一轮刷新即退出代卖
    eng = _engine(row=_Row("platform_only"), global_scope="full",
                  scopes=("full",))
    LiveEngine._refresh_sell_scope(eng)
    assert eng._sell_scope == "platform_only"


def test_refresh_sell_scope_null_falls_back_global_and_keeps_on_error():
    # NULL → 跟随全局
    eng = _engine(row=_Row(None), global_scope="full")
    LiveEngine._refresh_sell_scope(eng)
    assert eng._sell_scope == "full"
    # 读库异常 → 保持当前档位（fail-safe：不因读取失败扩大代卖范围）
    eng = _engine(row=_Row("full"), scopes=("platform_only",),
                  exc=RuntimeError("db down"))
    LiveEngine._refresh_sell_scope(eng)
    assert eng._sell_scope == "platform_only"
    assert eng.ctx.portfolios[0].sell_scope == "platform_only"


def test_apply_sell_scope_does_not_touch_orders_or_positions():
    """开关只改卖出数量口径：不撤单，也不改持仓数量/平台仓。"""
    class _Dispatcher:
        def __init__(self):
            self.calls = []

        def __getattr__(self, name):
            def _rec(*args, **kwargs):
                self.calls.append(name)
                return None
            return _rec

    pos = SimpleNamespace(quantity=1000, platform_quantity=300)
    port = SimpleNamespace(sell_scope="platform_only",
                           positions={"600000.SH": pos})
    disp = _Dispatcher()
    eng = SimpleNamespace(
        session_id=7, _sell_scope="platform_only",
        ctx=SimpleNamespace(portfolios=[port]),
        _dispatcher=disp, dispatcher=disp,
    )
    LiveEngine.apply_sell_scope(eng, "full")
    assert port.sell_scope == "full"
    assert pos.quantity == 1000 and pos.platform_quantity == 300
    assert disp.calls == []


# ---------------------------------------------------------------------------
# 按标的授权代卖：引擎每轮同步启用集合
# ---------------------------------------------------------------------------
class _ScopeFakeDB:
    """query().filter().all() 替身（LivePositionScope 启用集合）。"""

    def __init__(self, codes=None, exc=None):
        self._codes, self._exc = codes or [], exc
        self.closed = False

    def query(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def all(self):
        if self._exc is not None:
            raise self._exc
        return [(c,) for c in self._codes]

    def close(self):
        self.closed = True


def test_refresh_authorized_sets_codes_on_all_portfolios():
    ports = [SimpleNamespace(), SimpleNamespace()]
    eng = SimpleNamespace(
        session_id=7, ctx=SimpleNamespace(portfolios=ports),
        _db_session_factory=lambda: _ScopeFakeDB(["002745.SZ", "002922.SZ"]))
    LiveEngine._refresh_sell_scope_authorized(eng)
    expected = {"002745.SZ", "002922.SZ"}
    assert eng._authorized_sell_codes == expected
    assert [p.sell_scope_authorized for p in ports] == [expected, expected]


def test_refresh_authorized_empty_means_no_authorization():
    """无记录（默认）→ 空集合 = 维持 platform_only 原逻辑。"""
    port = SimpleNamespace(sell_scope_authorized={"600211.SH"})
    eng = SimpleNamespace(
        session_id=7, ctx=SimpleNamespace(portfolios=[port]),
        _db_session_factory=lambda: _ScopeFakeDB([]))
    LiveEngine._refresh_sell_scope_authorized(eng)
    assert port.sell_scope_authorized == set()


def test_refresh_authorized_keeps_previous_on_db_error():
    """读库失败沿用上一轮集合：不凭空扩大，也不把已授权票退回平台仓。"""
    port = SimpleNamespace(sell_scope_authorized={"600211.SH"})
    eng = SimpleNamespace(
        session_id=7, ctx=SimpleNamespace(portfolios=[port]),
        _db_session_factory=lambda: _ScopeFakeDB(exc=RuntimeError("db down")))
    LiveEngine._refresh_sell_scope_authorized(eng)
    assert port.sell_scope_authorized == {"600211.SH"}


def test_notify_sell_scope_skip_is_daily_first_per_code(monkeypatch):
    """按 (会话, 代码, 交易日) 每日首条：同代码重复触发只发一条 warn。"""
    calls = []
    from core.services import notify_service as ns
    monkeypatch.setattr(ns, "notify",
                        lambda *a, **k: calls.append((a, k)))
    eng = SimpleNamespace(session_id=7)
    sig = SimpleNamespace(stock_code="002922.SZ",
                          signal_type=SimpleNamespace(value="STOP_LOSS"))
    pos = SimpleNamespace(avg_cost=10.0, quantity=2500)
    bar = SimpleNamespace(bar_time=datetime(2026, 11, 23, 10, 0))
    LiveEngine._notify_sell_scope_skip(eng, sig, pos, bar, 2500, 8.0, -20.0)
    LiveEngine._notify_sell_scope_skip(eng, sig, pos, bar, 2500, 8.0, -20.0)
    assert len(calls) == 1
    args, kwargs = calls[0]
    assert args[0] == "warn"
    assert kwargs["gate"] == "sell_scope"
    assert kwargs["code"] == "002922.SZ"
    assert kwargs["trade_date"] == "2026-11-23"
    assert kwargs["fields"]["规则"] == "STOP_LOSS"
    assert kwargs["fields"]["请求数量"] == "2500"

    # 次日重新告警
    bar2 = SimpleNamespace(bar_time=datetime(2026, 11, 24, 10, 0))
    LiveEngine._notify_sell_scope_skip(eng, sig, pos, bar2, 2500, 7.8, -22.0)
    assert len(calls) == 2


# ---------------------------------------------------------------------------
# 服务侧：切换落库/留痕/即时生效/不改全局
# ---------------------------------------------------------------------------
def test_set_session_sell_scope_rejects_invalid_value():
    ok, data = svc.set_session_sell_scope(None, 1, "bad")
    assert ok is False
    assert data[0] == 400


def test_set_session_sell_scope_applies_to_running_engine_and_keeps_global(
        monkeypatch):
    cfg = {"tq_trade": {"live_sell_scope": "platform_only"}}
    monkeypatch.setattr(svc, "load_config", lambda: cfg)
    monkeypatch.setattr(
        svc, "evaluate_sell_scope_gate",
        lambda db, sid, engine=None: {"passed": True, "reasons": []})
    session = SimpleNamespace(id=1, name="会话1", sell_scope="platform_only",
                              status="running")
    db = _FakeDB(session)
    applied = []
    engine = SimpleNamespace(apply_sell_scope=lambda v: applied.append(v))
    ok, data = svc.set_session_sell_scope(
        db, 1, "full", reason="盘中扩围", engines={1: engine})
    assert ok is True
    assert session.sell_scope == "full"
    assert db.committed is True
    assert data["old"] == "platform_only" and data["new"] == "full"
    assert data["effective_at"] == "now"
    assert applied == ["full"]
    # 不反向改写全局配置
    assert cfg["tq_trade"]["live_sell_scope"] == "platform_only"


def test_set_session_sell_scope_rejected_when_gate_fails(monkeypatch):
    """对账闸不通过 → 拒绝开启，档位保持原值、不改库。"""
    gate = {"passed": False, "reasons": ["现金偏差 50 元超过容差 10 元"]}
    monkeypatch.setattr(
        svc, "evaluate_sell_scope_gate", lambda db, sid, engine=None: gate)
    session = SimpleNamespace(id=1, name="会话1", sell_scope="platform_only",
                              status="running")
    db = _FakeDB(session)
    applied = []
    ok, data = svc.set_session_sell_scope(
        db, 1, "full", engines={1: SimpleNamespace(
            apply_sell_scope=lambda v: applied.append(v))})
    assert ok is False
    assert data[0] == 409
    assert "现金偏差" in data[1]
    assert data[2] is gate
    assert session.sell_scope == "platform_only"
    assert db.committed is False
    assert applied == []


def test_sell_scope_gate_passes_within_tolerance():
    gate = svc._gate_from_payload({
        "cash_expected": "10403.99", "cash_actual": "10414.71",
        "cash_diff": "10.72",
        "equity_expected": "50000.00", "equity_actual": "50600.00",
        "detail": {
            "snapshot_at": "2026-11-19 10:00:00",
            "position_diffs": [
                {"stock_code": "600000.SH", "counter_qty": 1000,
                 "virtual_qty": 1000},
                # 池外仓（virtual=0）不影响开关
                {"stock_code": "000001.SZ", "counter_qty": 500,
                 "virtual_qty": 0},
            ],
        },
    }, "engine")
    assert gate["passed"] is True
    assert gate["position_diffs"] == [
        {"stock_code": "600000.SH", "counter_qty": 1000, "virtual_qty": 1000}]


def test_sell_scope_gate_rejects_cash_and_position_drift():
    gate = svc._gate_from_payload({
        "cash_expected": "10000", "cash_actual": "20000", "cash_diff": "10000",
        "equity_expected": "50000", "equity_actual": "50000",
        "detail": {"position_diffs": [
            {"stock_code": "600000.SH", "counter_qty": 900,
             "virtual_qty": 1000}]},
    }, "engine")
    assert gate["passed"] is False
    reasons = "；".join(gate["reasons"])
    assert "现金偏差" in reasons
    assert "600000.SH 柜台 900 股 ≠ 虚拟 1000 股" in reasons


def test_sell_scope_gate_rejects_stale_snapshot_in_trading_window():
    gate = svc._gate_from_payload({
        "cash_expected": "0", "cash_actual": "0", "cash_diff": "0",
        "equity_expected": "0", "equity_actual": "0",
        "detail": {"snapshot_at": "2020-01-01 09:30:00",
                   "position_diffs": []},
    }, "engine", require_fresh_snapshot=True)
    assert gate["passed"] is False
    assert "快照过期" in "；".join(gate["reasons"])


def test_set_session_sell_scope_unchanged_is_noop():
    session = SimpleNamespace(id=1, name="会话1", sell_scope="full",
                              status="stopped")
    db = _FakeDB(session)
    applied = []
    ok, data = svc.set_session_sell_scope(
        db, 1, "full", engines={1: SimpleNamespace(
            apply_sell_scope=lambda v: applied.append(v))})
    assert ok is True
    assert data["changed"] is False
    assert db.committed is False
    assert applied == []
