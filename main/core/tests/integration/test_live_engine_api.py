"""LiveEngine API 集成测试（0009 切片4）— TestClient + 内存 SQLite + Mock 桥。

验证 /api/live/sessions/{id}/start 接 LiveEngine（组装 portfolios + dispatcher + bar_poller
→ recover → start），/stop 停引擎，registry 按状态切换。
B4b：/orders、/trades、/positions 三个历史查询端点。
"""
from datetime import datetime
from decimal import Decimal
import json
import logging
import time

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.main import app
from core.config import load_config
from core.db import get_db
from core.models import (
    Base, StockPool, StockPoolStock, Formula, FormulaSignal,
    PortfolioStrategy, Strategy, LiveOrder, LiveTrade, LiveSessionPortfolio,
    LiveDecisionEvent, LiveSession,
)
from core.engine.position import Position
import core.api.live as live_api
from core.services import live_service as live_svc


@pytest.fixture(autouse=True)
def _isolate_stop_audit(tmp_path, monkeypatch):
    """stop 审计重定向到测试临时目录（不污染真实 logs/stop_audit.jsonl）。"""
    audit = tmp_path / "stop_audit.jsonl"
    monkeypatch.setattr(live_api, "_stop_audit_path", lambda: audit)
    return audit


@pytest.fixture
def client(tmp_path):
    """tmp 文件 SQLite（每会话独立连接）+ TestClient，覆盖 get_db。

    注：此前是内存 SQLite + StaticPool（所有 session 共享单条连接）——LiveEngine
    后台 tick 线程（_sweep_timeout_orders / persist_risk_state 等经 db_session_factory
    开会话）与 HTTP 请求线程会在同一连接上并发执行语句，互踩游标状态
    （sqlite3.InterfaceError: bad parameter or other API misuse / 事务交错），偶发
    幂等返回错乱。M1 多会话并行测试加重引擎启停密度后概率显著上升，故改为 tmp
    文件库：每会话独享连接，SQLite 文件锁 + busy timeout 天然串行化跨线程访问。
    """
    engine = create_engine(
        "sqlite:///%s" % (tmp_path / "test.db").as_posix(),
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    # 让 live API 的 db_session_factory 与 recover 用的 db 都指向测试库
    live_api.SessionLocal = Session
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c, Session
    app.dependency_overrides.clear()
    # 清理注册表 + 还原 SessionLocal
    live_api._ENGINES.clear()
    import core.db as db_mod
    live_api.SessionLocal = db_mod.SessionLocal


def _seed(db):
    """最小依赖链：StockPool(+成分股) → Formula → FormulaSignal → PortfolioStrategy → Strategy。"""
    pool = StockPool(code="TEST", name="test_pool")
    db.add(pool)
    db.flush()
    db.add(StockPoolStock(pool_id=pool.id, stock_code="600000.SH"))
    formula = Formula(name="open_formula", content="REF(CLOSE,1)")
    db.add(formula)
    db.flush()
    db.add(FormulaSignal(
        formula_id=formula.id, signal_name="open_sig",
        signal_type="OPEN", trigger_value=1,
    ))
    ps = PortfolioStrategy(
        name="test_portfolio", stock_pool_id=pool.id,
        initial_capital=Decimal("100000"),
        max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"),
    )
    db.add(ps)
    db.flush()
    db.add(Strategy(
        portfolio_id=ps.id, name="s1", formula_id=formula.id,
        period="1m", role="master",
        capital_ratio=Decimal("0.6"), max_positions=5,
        stop_loss_ratio=Decimal("0.05"), take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    ))
    db.commit()
    return ps.id


class _MockRecorder:
    """MockTransport：/ping、/order 返回成功，记录请求。"""

    def __init__(self):
        self.requests = []

    def handler(self, request):
        self.requests.append(request)
        path = request.url.path
        if path == "/order":
            return httpx.Response(200, json={"ok": True})
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {}})
        return httpx.Response(404, json={"ok": False})


@pytest.fixture
def mock_bridge(monkeypatch):
    """把 live API / live_service 构造的 HttpBridgeDispatcher 替换为 MockTransport 版本。

    engine=goldminer 时 start_session 走 live_service.make_dispatcher（模块内
    HttpBridgeDispatcher），而 _build_engine 直调走 live_api._dispatcher_cls()，
    两处都要 patch，否则会打到真实 7801。
    """
    rec = _MockRecorder()

    real_cls = live_api.HttpBridgeDispatcher

    def fake_constructor(base_url="http://127.0.0.1:8790", **kw):
        client = httpx.Client(transport=httpx.MockTransport(rec.handler))
        return real_cls(base_url=base_url, client=client, channel=kw.get("channel"))

    monkeypatch.setattr(live_api, "HttpBridgeDispatcher", fake_constructor)
    monkeypatch.setattr(live_svc, "HttpBridgeDispatcher", fake_constructor)
    return rec


def _create_session(c, name="live-test", portfolio_ids=(1,), mode="simulation",
                    execution_mode="auto", engine="goldminer", live_capital=None):
    """本文件为 HTTP 桥测试（mock_bridge）——显式钉住 engine="goldminer"（HTTP 桥
    通道），不依赖本地 config.yaml 的 tq_trade.default_engine（批次 A 后默认 tdx，
    落到 tdx 会走真实 TDX 预检/公式冒烟，桥 mock 完全失效）。"""
    payload = {
        "name": name, "portfolio_ids": list(portfolio_ids), "mode": mode,
        "execution_mode": execution_mode,
    }
    if engine:
        payload["engine"] = engine
    if live_capital is not None:
        payload["live_capital"] = live_capital
    resp = c.post("/api/live/sessions", json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    return body["data"]["id"]


def test_create_session_with_multiple_portfolios(client, mock_bridge):
    """POST /sessions 带 portfolio_ids → LiveSessionPortfolio 落多行;list_sessions 返回 portfolio_ids。"""
    c, Session = client
    db = Session()
    ps1 = _seed(db)
    # 再建一个组合,拿第二个 portfolio id
    pool2 = StockPool(code="TEST2", name="test_pool2")
    db.add(pool2)
    db.flush()
    db.add(StockPoolStock(pool_id=pool2.id, stock_code="000001.SZ"))
    formula2 = Formula(name="open_formula2", content="REF(CLOSE,1)")
    db.add(formula2)
    db.flush()
    ps2 = PortfolioStrategy(
        name="test_portfolio2", stock_pool_id=pool2.id,
        initial_capital=Decimal("200000"),
        max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"),
    )
    db.add(ps2)
    db.commit()
    ps2_id = ps2.id
    db.close()

    sid = _create_session(c, portfolio_ids=(ps1, ps2_id))

    # list_sessions 返回两个组合 id
    resp = c.get("/api/live/sessions")
    row = next(s for s in resp.json()["data"] if s["id"] == sid)
    assert sorted(row["portfolio_ids"]) == sorted([ps1, ps2_id])

    # get_session 同样返回
    detail = c.get("/api/live/sessions/%d" % sid).json()["data"]
    assert {p["portfolio_id"] for p in detail["portfolios"]} == {ps1, ps2_id}

    # 引擎组装:两个组合都进了 portfolios
    c.post("/api/live/sessions/%d/start" % sid)
    try:
        engine = live_api._ENGINES[sid]
        assert len(engine.portfolios) == 2
    finally:
        c.post("/api/live/sessions/%d/stop" % sid)


def test_start_session_runs_engine(client, mock_bridge):
    """POST /start → session status=running，registry 有引擎实例。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    sid = _create_session(c, portfolio_ids=(ps_id,))
    resp = c.post("/api/live/sessions/%d/start" % sid)
    assert resp.status_code == 200
    assert resp.json()["data"]["status"] == "running"
    assert sid in live_api._ENGINES

    # 停掉引擎，避免后台任务残留
    c.post("/api/live/sessions/%d/stop" % sid)


def _seed_extra_portfolio(db):
    """追加一个独立组合（池+成分股），返回组合 id（供多会话并行测试用）。"""
    pool = StockPool(code="TEST2", name="test_pool2")
    db.add(pool)
    db.flush()
    db.add(StockPoolStock(pool_id=pool.id, stock_code="000001.SZ"))
    ps = PortfolioStrategy(
        name="test_portfolio2", stock_pool_id=pool.id,
        initial_capital=Decimal("200000"),
        max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"),
    )
    db.add(ps)
    db.commit()
    return ps.id


class _FakeThsDispatcher:
    """THS 通道替身：heartbeat 通过 + file_builtin（绕过 OCR 启动闸）+ 空行情。"""
    channel = "ths_ui"
    execute_mode = "file_builtin"
    supports_batch_quote = False

    def __init__(self, **kw):
        self.init_kwargs = kw

    def heartbeat(self):
        return True

    def query_quote(self, code, period="1m", count=10):
        return []

    def query_calendar(self):
        return []

    def query_positions(self):
        return []


class _FakeTdxDispatcher:
    """TDX 通道替身：heartbeat 通过 + 空行情（不触真实通达信）。"""
    channel = "tdx_tq"
    supports_batch_quote = False

    def __init__(self, **kw):
        pass

    def heartbeat(self):
        return True

    def query_quote(self, code, period="1m", count=10):
        return []

    def query_calendar(self):
        return []

    def query_positions(self):
        return []


def test_two_non_ths_sessions_run_in_parallel(client, mock_bridge, monkeypatch):
    """M1 多会话并行：非 ths 的**不同引擎**（goldminer 桥 + tdx）可并行运行。

    同引擎双会话仍被 `_start_conflict` 409 拒绝（同一真实账户双份记账），
    故此处按「会话引擎不同」构造，与本文件其余桥测试一致。
    """
    c, Session = client
    db = Session()
    ps1 = _seed(db)
    ps2 = _seed_extra_portfolio(db)
    db.close()
    monkeypatch.setattr(live_svc, "TdxTqDispatcher", _FakeTdxDispatcher)

    sid1 = _create_session(c, portfolio_ids=(ps1,), engine="goldminer")
    sid2 = _create_session(c, portfolio_ids=(ps2,), engine="tdx")

    assert c.post("/api/live/sessions/%d/start" % sid1).json()["data"]["status"] == "running"
    assert c.post("/api/live/sessions/%d/start" % sid2).json()["data"]["status"] == "running"
    assert sid1 in live_api._ENGINES
    assert sid2 in live_api._ENGINES

    c.post("/api/live/sessions/%d/stop" % sid1)
    c.post("/api/live/sessions/%d/stop" % sid2)


def test_overlapping_portfolio_sessions_start_allowed(client, mock_bridge, monkeypatch):
    """M1 复制交易语义：同组合挂多个会话（不同引擎）→ 启动不拒（独立虚拟账本）。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()
    monkeypatch.setattr(live_svc, "TdxTqDispatcher", _FakeTdxDispatcher)

    sid1 = _create_session(c, portfolio_ids=(ps_id,), engine="goldminer")
    sid2 = _create_session(c, portfolio_ids=(ps_id,), engine="tdx")

    assert c.post("/api/live/sessions/%d/start" % sid1).json()["code"] == 0
    resp = c.post("/api/live/sessions/%d/start" % sid2)
    assert resp.status_code == 200
    assert resp.json()["code"] == 0
    assert resp.json()["data"]["status"] == "running"
    assert sid2 in live_api._ENGINES

    c.post("/api/live/sessions/%d/stop" % sid1)
    c.post("/api/live/sessions/%d/stop" % sid2)


def test_ths_second_session_rejected_channel_exclusive(client, mock_bridge, monkeypatch):
    """M1 THS 键鼠独占：ths 会话运行中，第二个 ths 会话 start → 409 拒绝。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()
    monkeypatch.setattr(live_svc, "ThsUiDispatcher", _FakeThsDispatcher)

    sid1 = _create_session(c, portfolio_ids=(ps_id,), engine="ths")
    sid2 = _create_session(c, portfolio_ids=(ps_id,), engine="ths")

    assert c.post("/api/live/sessions/%d/start" % sid1).json()["code"] == 0
    assert sid1 in live_api._ENGINES

    resp = c.post("/api/live/sessions/%d/start" % sid2)
    assert resp.status_code == 200  # 统一响应格式，业务错误在 body.code
    assert resp.json()["code"] != 0
    assert "同花顺" in resp.json()["message"]
    assert sid2 not in live_api._ENGINES
    assert sid1 in live_api._ENGINES  # 原会话不受影响

    c.post("/api/live/sessions/%d/stop" % sid1)


def test_ths_running_tdx_session_start_requires_keyboard_ack(client, mock_bridge, monkeypatch):
    """1.0.5 键鼠软拦：ths 运行中启动 tdx 先 409，带 ack 才放行。

    C1 引擎类型独占不拦（ths/tdx 不同引擎）、C2 账户键不同也不拦；真正触发的是
    C3 键鼠 UI 自动化互斥（两者都操作交易窗口，同时启动有误点风险）。1.0.5 起
    默认软拦：未带 ``ack_keyboard_risk`` → 409 rule=keyboard_exclusive，
    带 ack → 放行并留 WARNING + 决策事件。旧断言「直接 code=0」已作废。

    一句话新口径：ths 与 tdx 同属键鼠通道，同时运行默认软拦（409），
    必须带 ack_keyboard_risk=true 显式确认才放行。
    """
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()
    monkeypatch.setattr(live_svc, "ThsUiDispatcher", _FakeThsDispatcher)
    monkeypatch.setattr(live_svc, "TdxTqDispatcher", _FakeTdxDispatcher)

    # 本机配置若开 keyboard_conflict_hard_block=true，ack 也会 409；测试钉住软拦口径。
    real_load_config = live_svc.load_config

    def _soft_block_config():
        cfg = dict(real_load_config())
        tq = dict(cfg.get("tq_trade") or {})
        tq["keyboard_conflict_hard_block"] = False
        cfg["tq_trade"] = tq
        return cfg

    monkeypatch.setattr(live_svc, "load_config", _soft_block_config)

    sid1 = _create_session(c, portfolio_ids=(ps_id,), engine="ths")
    sid2 = _create_session(c, portfolio_ids=(ps_id,), engine="tdx")

    assert c.post("/api/live/sessions/%d/start" % sid1).json()["code"] == 0
    body = c.post("/api/live/sessions/%d/start" % sid2).json()
    assert body["code"] == 409
    assert body["data"]["rule"] == "keyboard_exclusive"
    assert body["data"]["requires_ack"] is True
    assert sid2 not in live_api._ENGINES
    assert sid1 in live_api._ENGINES  # 原会话不受影响

    acked = c.post("/api/live/sessions/%d/start" % sid2,
                   json={"ack_keyboard_risk": True}).json()
    assert acked["code"] == 0
    assert acked["data"]["status"] == "running"
    assert sid2 in live_api._ENGINES

    c.post("/api/live/sessions/%d/stop" % sid1)
    c.post("/api/live/sessions/%d/stop" % sid2)


def test_restart_same_running_session_is_idempotent(client, mock_bridge):
    """B6：同一 session 重复 start → 幂等返回 running（非拒绝）。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    sid = _create_session(c, portfolio_ids=(ps_id,))
    c.post("/api/live/sessions/%d/start" % sid)
    resp = c.post("/api/live/sessions/%d/start" % sid)
    assert resp.json()["code"] == 0
    assert resp.json()["data"]["status"] == "running"

    c.post("/api/live/sessions/%d/stop" % sid)


def test_stop_session_stops_engine(client, mock_bridge, monkeypatch):
    """/stop → 先 returning stopping，异步收敛后落 stopped，registry 清空引擎。

    1.0.5 起 stop 为「同步置 stopping（仍占用 C1/C2）→ engine.stop() 协程收敛 →
    finalize_stopped 置 stopped」。因此响应里是中间态 stopping，只有后台收尾完成
    才变 stopped——不再同步返回 stopped。

    一句话新口径：/stop 先返回 stopping，后台 engine.stop() 收敛完成后才落 stopped；
    stopping 期间仍占用引擎/账户，只有 stopped 才释放。
    """
    c, Session = client
    # finalize_stopped 的工厂默认参数在定义时绑定（core.db.SessionLocal），测试覆盖
    # 不到；显式注入测试 Session，否则后台收尾会写回真实 dev.db、测试库永远 stopping。
    real_finalize = live_svc.finalize_stopped
    monkeypatch.setattr(
        live_svc, "finalize_stopped",
        lambda sid: real_finalize(sid, db_session_factory=Session))
    db = Session()
    ps_id = _seed(db)
    db.close()

    sid = _create_session(c, portfolio_ids=(ps_id,))
    c.post("/api/live/sessions/%d/start" % sid)
    assert sid in live_api._ENGINES

    resp = c.post("/api/live/sessions/%d/stop" % sid)
    assert resp.status_code == 200
    assert resp.json()["data"]["status"] == "stopping"
    # 注册表同步弹出（不再持有引擎），DB 状态由后台协程收尾
    assert sid not in live_api._ENGINES

    # engine.stop() 可能在收尾前拉一次账户快照（桥/TDX 超时约 5s），留足收敛时间
    deadline = time.monotonic() + 20.0
    status = None
    while time.monotonic() < deadline:
        status = c.get("/api/live/sessions/%d" % sid).json()["data"]["status"]
        if status == "stopped":
            break
        time.sleep(0.1)
    assert status == "stopped"


def test_stop_session_is_idempotent(client, mock_bridge):
    """已停止的 session 再次 /stop → 仍返回 200（不是 404）。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    sid = _create_session(c, portfolio_ids=(ps_id,))
    c.post("/api/live/sessions/%d/start" % sid)
    assert c.post("/api/live/sessions/%d/stop" % sid).json()["code"] == 0

    resp = c.post("/api/live/sessions/%d/stop" % sid)
    assert resp.status_code == 200
    assert resp.json()["code"] == 0
    assert resp.json()["data"]["status"] == "stopped"


def test_stop_writes_structured_audit(client, mock_bridge, _isolate_stop_audit):
    """/stop 落一条结构化审计：会话/来源 IP:port/结果/停止前状态/无鉴权说明。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    sid = _create_session(c, portfolio_ids=(ps_id,))
    c.post("/api/live/sessions/%d/start" % sid)
    c.post("/api/live/sessions/%d/stop" % sid)

    lines = _isolate_stop_audit.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert row["session_id"] == sid
    assert row["session_name"] == "live-test"
    assert row["result"] == "success"
    assert row["status_before"] == "running"
    assert row["status_after"] == "stopping"
    assert row["operator"] == "无鉴权，无法识别操作者"
    assert row["source_ip"] == "testclient"
    assert row["source_port"] is not None
    assert "命中引擎" in row["reason"]
    assert row["process"].startswith("pid=")
    assert row["ts"]

    # 留痕可从接口查到（与直接看 logs/stop_audit.jsonl 等价）
    resp = c.get("/api/live/sessions/%d/stop-audits" % sid)
    items = resp.json()["data"]["items"]
    assert len(items) == 1
    assert items[0]["session_id"] == sid


def test_stop_audit_records_failure_for_missing_session(
        client, _isolate_stop_audit):
    """会话不存在 → 请求失败也要留痕（result=failed + 原因），不伪造状态。"""
    c, _Session = client
    resp = c.post("/api/live/sessions/999999/stop")
    assert resp.status_code == 200
    assert resp.json()["code"] == 404

    lines = _isolate_stop_audit.read_text(encoding="utf-8").splitlines()
    row = json.loads(lines[-1])
    assert row["session_id"] == 999999
    assert row["result"] == "failed"
    assert row["status_before"] is None
    assert row["status_after"] is None
    assert "不存在" in row["reason"]


def test_stop_with_missing_engine_warns_process_tag(client, caplog):
    """DB 显示 running 但本进程无引擎（另一进程持有）→ 必须 WARNING 带 pid/port。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    sid = _create_session(c, portfolio_ids=(ps_id,))
    db = Session()
    row = db.get(LiveSession, sid)
    row.status = "running"  # 模拟 stop 打到了没有引擎的进程
    db.commit()
    db.close()

    with caplog.at_level(logging.WARNING, logger="core.api.live"):
        resp = c.post("/api/live/sessions/%d/stop" % sid)
    assert resp.status_code == 200
    assert resp.json()["code"] == 0
    assert resp.json()["data"]["status"] == "stopped"

    # 只看「stop 请求」诊断行：审计 JSON 里也会带同样的原因文字（不参与本断言）
    msgs = [r.getMessage() for r in caplog.records
            if r.getMessage().startswith("stop 请求")
            and "注册表未找到" in r.getMessage()]
    assert len(msgs) == 1
    assert "pid=" in msgs[0] and "port=" in msgs[0]
    assert "另一后端进程" in msgs[0]


def test_engine_registry_diagnostic_lists_pid_and_db_status(client, mock_bridge):
    """只读诊断接口并排返回本进程引擎与 DB 状态，便于排查进程错配。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    sid = _create_session(c, portfolio_ids=(ps_id,))
    c.post("/api/live/sessions/%d/start" % sid)
    try:
        data = c.get("/api/live/engine-registry").json()["data"]
        assert data["pid"] > 0
        assert "port" in data
        item = next(e for e in data["engines"] if e["session_id"] == sid)
        assert item["running"] is True
        assert item["db_status"] == "running"
        assert item["mismatch"] is False
        assert all(x["session_id"] != sid
                   for x in data["db_active_without_engine"])
    finally:
        c.post("/api/live/sessions/%d/stop" % sid)


def test_create_session_rejects_duplicate_and_missing_portfolio(client, mock_bridge):
    """portfolio_ids 重复/不存在 → 400 业务错误，而不是 FK/唯一约束的 500。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    dup = c.post("/api/live/sessions",
                 json={"name": "dup", "portfolio_ids": [ps_id, ps_id]})
    assert dup.status_code == 200
    assert dup.json()["code"] != 0
    assert "重复" in dup.json()["message"]

    missing = c.post("/api/live/sessions",
                     json={"name": "missing", "portfolio_ids": [999999]})
    assert missing.status_code == 200
    assert missing.json()["code"] != 0
    assert "不存在" in missing.json()["message"]


def test_bridge_status_endpoint(client, mock_bridge):
    """未运行 → online=None；运行中 → online=heartbeat()。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    sid = _create_session(c, portfolio_ids=(ps_id,))
    # 未运行
    resp = c.get("/api/live/sessions/%d/bridge-status" % sid)
    assert resp.status_code == 200
    assert resp.json()["data"]["online"] is None
    assert resp.json()["data"]["status"] == "not_running"

    # 启动后桥在线（mock /ping 返回 ok）
    c.post("/api/live/sessions/%d/start" % sid)
    resp = c.get("/api/live/sessions/%d/bridge-status" % sid)
    assert resp.json()["data"]["online"] is True

    c.post("/api/live/sessions/%d/stop" % sid)


def test_get_session_includes_bridge_status(client, mock_bridge):
    """G7（0011 §5.11）：GET /sessions/{id} 并入 bridge_online/pending_orders/last_backfill_time。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    sid = _create_session(c, portfolio_ids=(ps_id,))
    # 未运行 → 三字段空值
    body = c.get("/api/live/sessions/%d" % sid).json()["data"]
    assert body["bridge_online"] is None
    assert body["pending_orders"] == 0
    assert body["last_backfill_time"] is None

    # 运行中 → bridge_online 实时心跳（mock /ping ok），pending/last_backfill 键存在
    c.post("/api/live/sessions/%d/start" % sid)
    body = c.get("/api/live/sessions/%d" % sid).json()["data"]
    assert body["bridge_online"] is True
    assert body["pending_orders"] == 0
    assert body["last_backfill_time"] is None

    c.post("/api/live/sessions/%d/stop" % sid)


def test_get_and_list_sessions_include_reconcile_mismatches(client, mock_bridge):
    """D-3（I-7）：session 详情/列表序列化并入 reconcile_mismatches——
    运行中取引擎内存最近一次对账差异摘要；未运行（无引擎）为 []。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    sid = _create_session(c, portfolio_ids=(ps_id,))
    # 未运行 → []
    body = c.get("/api/live/sessions/%d" % sid).json()["data"]
    assert body["reconcile_mismatches"] == []

    c.post("/api/live/sessions/%d/start" % sid)
    engine = live_api._ENGINES[sid]
    engine._reconcile_mismatches.append(
        {"code": "600000.SH", "virtual": 1000, "real": 800, "diff": 200}
    )
    # 详情 → 引擎内存差异
    body = c.get("/api/live/sessions/%d" % sid).json()["data"]
    assert body["reconcile_mismatches"] == [
        {"code": "600000.SH", "virtual": 1000, "real": 800, "diff": 200},
    ]
    # 列表 → 同样并入（service 层从注册表取）
    row = next(s for s in c.get("/api/live/sessions").json()["data"] if s["id"] == sid)
    assert row["reconcile_mismatches"] == [
        {"code": "600000.SH", "virtual": 1000, "real": 800, "diff": 200},
    ]

    c.post("/api/live/sessions/%d/stop" % sid)
    # 停止（引擎已弹出注册表）→ []
    body = c.get("/api/live/sessions/%d" % sid).json()["data"]
    assert body["reconcile_mismatches"] == []


def test_sse_line_formats_event():
    """B5：_sse_line 把 {type, **payload} 格式化为 SSE 行（data 去掉 type 字段）。"""
    line = live_api._sse_line({"type": "signal", "portfolio_id": 1, "strategy_id": 1})
    assert line == "event: signal\ndata: {\"portfolio_id\": 1, \"strategy_id\": 1}\n\n"
    ping = live_api._sse_line({"type": "ping", "time": "2026-08-05T14:30:00"})
    assert ping.startswith("event: ping\ndata: ")
    assert "\"time\"" in ping


def test_heartbeat_stream_yields_ping_when_idle():
    """B5：未运行 session 的心跳流（_heartbeat_stream）发 event: ping 保活。"""
    import asyncio

    class _FakeRequest:
        async def is_disconnected(self):
            return False

    async def drive():
        gen = live_api._heartbeat_stream(_FakeRequest(), 1)
        it = gen.__aiter__()
        first = await it.__anext__()
        assert first.startswith("event: ping\ndata: ")
        assert "\"time\"" in first
        await gen.aclose()

    asyncio.run(drive())


def test_heartbeat_stream_emits_reconnect_when_engine_registers():
    """D-5（前端 I4）：心跳流在引擎注册（session 启动）后发 event: reconnect 并结束——
    前端收到后关闭当前 EventSource 重连，落到真事件流。"""
    import asyncio

    class _FakeRequest:
        async def is_disconnected(self):
            return False

    async def drive():
        gen = live_api._heartbeat_stream(_FakeRequest(), 4242, ping_interval=0.01)
        it = gen.__aiter__()
        first = await it.__anext__()
        assert first.startswith("event: ping\ndata: ")
        # 引擎注册（值本身不重要，非 None 即视为运行中）
        live_api._ENGINES[4242] = object()
        try:
            second = await it.__anext__()
            assert second.startswith("event: reconnect\ndata: ")
            assert '"session_id": 4242' in second
            # 发完 reconnect 流即结束（客户端 EventSource 触发重连）
            with pytest.raises(StopAsyncIteration):
                await it.__anext__()
        finally:
            live_api._ENGINES.pop(4242, None)

    asyncio.run(drive())


def test_stream_endpoint_pings_when_not_running(client, mock_bridge, monkeypatch):
    """B5：未运行 session → /stream 走 _heartbeat_stream，返回 SSE 心跳。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()
    sid = _create_session(c, portfolio_ids=(ps_id,))

    async def fake_heartbeat(request, session_id):
        yield "event: ping\ndata: {\"time\": \"x\"}\n\n"
    monkeypatch.setattr(live_api, "_heartbeat_stream", fake_heartbeat)

    resp = c.get("/api/live/sessions/%d/stream" % sid)
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    assert "event: ping" in resp.text


def test_stream_endpoint_relays_engine_events_when_running(client, mock_bridge):
    """B5：运行中 session → /stream 转发引擎 stream_events（引擎事件 → SSE 行）。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()
    sid = _create_session(c, portfolio_ids=(ps_id,))
    c.post("/api/live/sessions/%d/start" % sid)
    engine = live_api._ENGINES[sid]

    async def fake_stream():
        yield {"type": "signal", "portfolio_id": 1, "strategy_id": 1, "signal_name": "open_sig"}
        yield {"type": "ping", "time": "2026-08-05T14:30:00"}
    engine.stream_events = fake_stream  # 实例属性遮蔽方法，端点消费该有限流

    resp = c.get("/api/live/sessions/%d/stream" % sid)
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    assert "event: signal" in resp.text
    assert '"portfolio_id": 1' in resp.text
    assert '"signal_name": "open_sig"' in resp.text
    assert "event: ping" in resp.text

    c.post("/api/live/sessions/%d/stop" % sid)


def test_stream_endpoint_404_when_session_missing(client):
    """#13/#5：session 不存在 → HTTP 404（HTTPException pass-through，非 body-code）。

    EventSource 需真实 HTTP 错误码才触发 onerror，body-code+200 会让前端静默失败。
    全局 Exception 处理器不拦截 HTTPException，此行为保持。
    """
    c, _ = client
    resp = c.get("/api/live/sessions/9999/stream")
    assert resp.status_code == 404
    # HTTPException pass-through：响应体是 {"detail":...}，非统一 envelope
    assert "session" in resp.json()["detail"]


def test_build_engine_fills_formula_mapping(client, mock_bridge):
    """_build_engine 后 LiveEngine 持有 _formula_by_strategy（strategy_id → formula_name）。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)  # 建 Formula(open_formula) + Strategy(formula_id)
    db.close()

    # 建 session + link（_build_engine 按 session_id 查 LiveSessionPortfolio）
    sid = _create_session(c, portfolio_ids=(ps_id,))

    # 直接调 _build_engine（不经 /start，避免起后台任务）
    db2 = Session()
    try:
        engine = live_api._build_engine(sid, db2)
    finally:
        db2.close()

    # 策略 1 的公式名 = open_formula（_seed 建的）
    assert 1 in engine._formula_by_strategy
    assert engine._formula_by_strategy[1] == "open_formula"
    # tq_formula 已注入
    assert engine._tq_formula is not None
    assert engine._formula_count == 200
    # #27：formula_count 按公式配 → 引擎收到 {formula_name: count}（_seed 默认 200）
    assert engine._formula_count_by_name == {"open_formula": 200}


# ---- 0011 外部信号注入：signal_file_path → build_engine 接线 ----

def test_create_and_build_engine_wires_signal_file_source(client, mock_bridge):
    """POST /sessions 带 signal_file_path → 落库；_build_engine 建信号源
    （公式名集合 = formula_by_strategy 值集）；未配置会话 → _signal_source is None。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    resp = c.post("/api/live/sessions", json={
        "name": "ext", "portfolio_ids": [ps_id], "mode": "simulation",
        "execution_mode": "auto", "engine": "goldminer",
        "signal_file_path": "D:/ths/alert.txt",
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    sid = body["data"]["id"]

    db2 = Session()
    try:
        engine = live_api._build_engine(sid, db2)
        assert engine._signal_source is not None
        assert engine._signal_source._formula_names == {"open_formula"}
    finally:
        db2.close()

    # 未配置 signal_file_path 的会话 → 不启用（引擎行为不变）
    sid2 = _create_session(c, name="no-ext", portfolio_ids=(ps_id,))
    db3 = Session()
    try:
        engine2 = live_api._build_engine(sid2, db3)
        assert engine2._signal_source is None
    finally:
        db3.close()


# ---- B4b: 历史查询端点（orders / trades / positions）----

def _add_order(db, sid, stock="600000.SH", status="filled", trade_type="BUY",
               qty=100, price="10.5", signal_name="open_sig", signal_type="OPEN",
               filled_qty=None):
    """插入一条 LiveOrder，返回实例（未 commit，由调用方控制）。"""
    return LiveOrder(
        live_session_id=sid, portfolio_strategy_id=1, strategy_id=1,
        stock_code=stock, trade_type=trade_type, order_type="LIMIT",
        price=Decimal(price), quantity=qty,
        filled_quantity=qty if filled_qty is None else filled_qty,
        filled_price=Decimal(price) if filled_qty is None else None,
        status=status, signal_name=signal_name, signal_type=signal_type,
        bar_time=datetime(2026, 8, 5, 10, 30),
    )


def _add_trade(db, sid, stock="600000.SH", trade_type="BUY", qty=100, price="10.5",
               trade_time=None, order=None):
    """插入一条 LiveTrade，返回实例（未 commit）。"""
    p = Decimal(price)
    return LiveTrade(
        live_session_id=sid,
        live_order_id=order.id if order else None,
        portfolio_strategy_id=1, strategy_id=1,
        stock_code=stock, trade_type=trade_type,
        price=p, quantity=qty, amount=p * qty,
        commission=Decimal("0.50"), stamp_duty=Decimal("0"),
        trade_time=trade_time or datetime(2026, 8, 5, 10, 31),
    )


def test_query_orders_history(client, mock_bridge):
    """GET /sessions/{id}/orders → 返回全部委托；?status= 过滤。"""
    c, Session = client
    sid = _create_session(c, portfolio_ids=())
    db = Session()
    db.add_all([
        _add_order(db, sid, status="filled"),
        _add_order(db, sid, status="submitted", trade_type="SELL", qty=200),
    ])
    db.commit()
    db.close()

    resp = c.get("/api/live/sessions/%d/orders" % sid)
    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    assert len(body["data"]) == 2
    assert {o["status"] for o in body["data"]} == {"filled", "submitted"}
    # 同 created_at 时按 id 倒序 → 新单(submitted)在前
    assert body["data"][0]["status"] == "submitted"
    assert body["data"][1]["status"] == "filled"

    resp2 = c.get("/api/live/sessions/%d/orders?status=submitted" % sid)
    rows = resp2.json()["data"]
    assert len(rows) == 1
    assert rows[0]["trade_type"] == "SELL"

    # 会话不存在 → body code 404
    resp3 = c.get("/api/live/sessions/9999/orders")
    assert resp3.json()["code"] == 404


def test_query_orders_returns_empty_list_when_none(client, mock_bridge):
    """无任何委托 → data 为空列表（非 null）。"""
    c, Session = client
    sid = _create_session(c, portfolio_ids=())
    resp = c.get("/api/live/sessions/%d/orders" % sid)
    assert resp.json()["code"] == 0
    assert resp.json()["data"] == []


def test_query_trades_history(client, mock_bridge):
    """GET /sessions/{id}/trades → 成交明细按 trade_time 倒序。"""
    c, Session = client
    sid = _create_session(c, portfolio_ids=())
    db = Session()
    lo = _add_order(db, sid)
    db.add(lo)
    db.flush()
    db.add_all([
        _add_trade(db, sid, order=lo, trade_time=datetime(2026, 8, 5, 10, 31)),
        _add_trade(db, sid, trade_type="SELL", qty=100, price="11.0",
                   trade_time=datetime(2026, 8, 5, 10, 35)),
    ])
    db.commit()
    db.close()

    resp = c.get("/api/live/sessions/%d/trades" % sid)
    body = resp.json()
    assert body["code"] == 0
    assert len(body["data"]) == 2
    # 倒序：SELL(10:35) 在前
    assert body["data"][0]["trade_type"] == "SELL"
    assert body["data"][1]["trade_type"] == "BUY"
    assert body["data"][1]["price"] == 10.5
    assert body["data"][1]["amount"] == 1050.0


def test_query_trades_history_date_filter(client, mock_bridge):
    """GET /trades?start_date=&end_date= 按北京自然日过滤；缺省仍全量（向后兼容）。"""
    c, Session = client
    sid = _create_session(c, portfolio_ids=())
    db = Session()
    lo = _add_order(db, sid)
    db.add(lo)
    db.flush()
    db.add_all([
        _add_trade(db, sid, order=lo, trade_time=datetime(2026, 8, 4, 14, 50)),
        _add_trade(db, sid, order=lo, trade_time=datetime(2026, 8, 5, 10, 35)),
        _add_trade(db, sid, order=lo, trade_type="SELL", qty=100, price="11.0",
                   trade_time=datetime(2026, 8, 6, 9, 31)),
    ])
    db.commit()
    db.close()

    url = "/api/live/sessions/%d/trades" % sid
    # 单日闭区间：只回 8 月 5 日那一笔
    body = c.get(url + "?start_date=2026-08-05&end_date=2026-08-05").json()
    assert body["code"] == 0
    assert len(body["data"]) == 1
    assert body["data"][0]["trade_time"].startswith("2026-08-05")
    # 只给起点 → 下界过滤（8/5 起两笔）
    assert len(c.get(url + "?start_date=2026-08-05").json()["data"]) == 2
    # 缺省 → 全量，与旧行为一致（不删除任何成交）
    assert len(c.get(url).json()["data"]) == 3
    # 非法日期格式 → 400
    assert c.get(url + "?start_date=2026/08/05").json()["code"] == 400


def test_query_positions_when_stopped_aggregates_from_trades(client, mock_bridge):
    """未运行 → /positions 从 live_trades 重放聚合（BUY 加、SELL 减，均价加权）。

    600@10 BUY + 200@12 BUY + 100@11 SELL → 净 700 股，均价 (600*10+200*12)/800=10.5。
    """
    c, Session = client
    sid = _create_session(c, portfolio_ids=())
    db = Session()
    db.add_all([
        _add_trade(db, sid, qty=600, price="10"),
        _add_trade(db, sid, qty=200, price="12"),
        _add_trade(db, sid, trade_type="SELL", qty=100, price="11"),
    ])
    db.commit()
    db.close()

    resp = c.get("/api/live/sessions/%d/positions" % sid)
    body = resp.json()
    assert body["code"] == 0
    row = next(p for p in body["data"] if p["stock_code"] == "600000.SH")
    assert row["quantity"] == 700
    # 重建口径含柜台真实费用（2026-11-11 起与实盘一致）：
    # (600*10 + 200*12 + 2×0.5 佣金) / 800 = 10.50125 → 10.501
    assert row["avg_cost"] == pytest.approx(10.501, abs=1e-6)
    assert row["market_value"] == pytest.approx(7350.88, abs=0.01)  # 700×10.50125 四舍五入到分
    # 归属：_add_trade 恒为 portfolio_strategy_id=1/strategy_id=1
    assert row["portfolio_id"] == 1
    assert row["strategy_id"] == 1


def test_query_positions_splits_rows_by_strategy(client, mock_bridge):
    """同票被两个子策略持有 → 停止态重放按 (组合, 子策略) 拆成两行。"""
    c, Session = client
    sid = _create_session(c, portfolio_ids=())
    db = Session()
    tr1 = _add_trade(db, sid, qty=300, price="10")
    tr2 = _add_trade(db, sid, qty=200, price="12")
    tr2.strategy_id = 2  # 第二个子策略持有同一只票
    db.add_all([tr1, tr2])
    db.commit()
    db.close()

    resp = c.get("/api/live/sessions/%d/positions" % sid)
    body = resp.json()
    assert body["code"] == 0
    rows = [p for p in body["data"] if p["stock_code"] == "600000.SH"]
    assert len(rows) == 2
    by_sid = {r["strategy_id"]: r for r in rows}
    assert by_sid[1]["quantity"] == 300
    assert by_sid[2]["quantity"] == 200


def test_query_positions_uses_engine_when_running(client, mock_bridge):
    """运行中 → /positions 读引擎内存态虚拟持仓（含未落库的当日变动），带归属 id。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()
    sid = _create_session(c, portfolio_ids=(ps_id,))
    c.post("/api/live/sessions/%d/start" % sid)
    try:
        engine = live_api._ENGINES[sid]
        # 直接往引擎内存态塞一个持仓（模拟当日已成交未落库/或恢复态）
        pos = Position("600000.SH")
        pos.buy(600, Decimal("10.5"))
        engine.portfolios[0].strategies[0].positions["600000.SH"] = pos

        resp = c.get("/api/live/sessions/%d/positions" % sid)
        body = resp.json()
        assert body["code"] == 0
        row = next(p for p in body["data"] if p["stock_code"] == "600000.SH")
        assert row["quantity"] == 600
        assert row["avg_cost"] == 10.5
        # 归属 id 与引擎内存态一致（不硬编码，测试库里 id 未必为 1）
        assert row["portfolio_id"] == engine.portfolios[0].portfolio_id
        assert row["strategy_id"] == engine.portfolios[0].strategies[0].strategy_id
    finally:
        c.post("/api/live/sessions/%d/stop" % sid)


# ---- 双桥：_bridge_config 返回双址；_build_engine 按 session.mode 选桥 ----

def _configured_bridge_urls():
    """按 config.yaml 的 iquant_bridge 段取双桥址（缺省回退与 live_service 一致）。

    不硬编码 8790/8791：live 腿可能指向普通账户（8790）或信用账户（8791），
    用例必须跟随配置，而不是要求配置迎合用例。
    """
    cfg = load_config()
    br = cfg.get("iquant_bridge", {}) if isinstance(cfg, dict) else {}
    br = br if isinstance(br, dict) else {}
    sim = br.get("simulation", {}) if isinstance(br.get("simulation"), dict) else {}
    liv = br.get("live", {}) if isinstance(br.get("live"), dict) else {}
    return (sim.get("base_url") or "http://127.0.0.1:8790",
            liv.get("base_url") or "http://127.0.0.1:8791")


def test_bridge_config_returns_dual_urls():
    """_bridge_config() 返回 simulation/live 两个 base_url（跟随 config.yaml 双桥结构）。"""
    expected_sim, expected_live = _configured_bridge_urls()
    br = live_api._bridge_config()
    assert br["simulation"] == expected_sim
    assert br["live"] == expected_live


def test_build_engine_picks_bridge_by_mode(client, monkeypatch):
    """_build_engine(mode) 按 mode 选桥址：simulation→仿真桥，live→实盘桥，
    未知 mode 兜底仿真桥（防误传走到真实资金桥）。

    simulation/live 用真实创建对应 mode 的 session；unknown mode 不会被
    create_session 接受（白名单），故用 simulation session 直接调 _build_engine
    传 unknown，验证兜底分支。
    """
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    sim_url, live_url = _configured_bridge_urls()
    captured = {}

    real_cls = live_api.HttpBridgeDispatcher

    def fake_constructor(base_url="http://127.0.0.1:8790", **kw):
        captured["base_url"] = base_url
        # 复用 mock_bridge 的 MockTransport 思路，给一个能 /ping 的 client
        client_ = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(200, json={"ok": True})))
        return real_cls(base_url=base_url, client=client_)

    monkeypatch.setattr(live_api, "HttpBridgeDispatcher", fake_constructor)
    # 显式钉全局通道选择：本地 config.yaml ths_ui.enabled=true 时 _dispatcher_cls()
    # 会返回 ThsUiDispatcher，绕过上面的类属性 patch（消除对本地配置的隐性依赖）。
    monkeypatch.setattr(live_api, "_dispatcher_cls", lambda: fake_constructor)

    # simulation / live：创建对应 mode 的 session 后直接调 _build_engine
    # （显式钉 engine=goldminer：HTTP 桥通道，消除对本地 default_engine 的隐性依赖）
    for mode, expected in [("simulation", sim_url), ("live", live_url)]:
        sid = _create_session(c, portfolio_ids=(ps_id,), mode=mode, engine="goldminer")
        db2 = Session()
        try:
            live_api._build_engine(sid, db2, mode)
        finally:
            db2.close()
        assert captured["base_url"] == expected, "mode=%s 应选 %s，实际 %s" % (mode, expected, captured["base_url"])

    # unknown：create_session 白名单会拒，用已存在的 simulation session 直接调 _build_engine
    # 传 unknown mode，验证兜底走仿真桥（防误传走到真实资金桥）。
    sim_sid = _create_session(c, portfolio_ids=(ps_id,), mode="simulation", engine="goldminer")
    db3 = Session()
    try:
        live_api._build_engine(sim_sid, db3, "unknown")
    finally:
        db3.close()
    assert captured["base_url"] == sim_url, "unknown mode 应兜底仿真桥，实际 %s" % captured["base_url"]


def test_start_session_routes_engine_to_bridge(client, monkeypatch):
    """start_session 按会话引擎构造桥：goldminer 走 config.goldminer.base_url（7801）。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    captured = {}
    real_cls = live_api.HttpBridgeDispatcher

    def fake_constructor(base_url="http://127.0.0.1:8790", **kw):
        captured["base_url"] = base_url
        client_ = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(200, json={"ok": True})))
        return real_cls(base_url=base_url, client=client_, channel=kw.get("channel"))

    monkeypatch.setattr(live_api, "HttpBridgeDispatcher", fake_constructor)
    # 启动路由经 live_service.make_dispatcher 构造（模块内全局），两处都要 patch
    monkeypatch.setattr(live_svc, "HttpBridgeDispatcher", fake_constructor)

    # 显式 engine=goldminer（HTTP 桥），不依赖全局 default_engine（tdx）
    sid = _create_session(c, portfolio_ids=(ps_id,), mode="live", engine="goldminer")
    resp = c.post("/api/live/sessions/%d/start" % sid)
    assert resp.status_code == 200
    assert resp.json()["code"] == 0
    assert captured["base_url"] == live_svc.goldminer_config()["base_url"]
    c.post("/api/live/sessions/%d/stop" % sid)


def test_create_session_rejects_invalid_mode(client, mock_bridge):
    """create_session 对非法 mode 返回业务错误（白名单 simulation|live）。"""
    c, _ = client
    resp = c.post("/api/live/sessions", json={"name": "bad", "portfolio_ids": [], "mode": "real"})
    body = resp.json()
    assert body["code"] != 0
    assert "mode" in body["message"]


# ---- 会话级实盘资金基准（live_capital）：None=组合初始资金；设置=全组合以该值起账 ----

def test_create_session_persists_live_capital(client, mock_bridge):
    """create_session 落库 live_capital；list/detail 序列化返回；未传 → None（现行为）。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    sid = _create_session(c, portfolio_ids=(ps_id,), live_capital=1000000)
    detail = c.get("/api/live/sessions/%d" % sid).json()["data"]
    assert float(detail["live_capital"]) == 1000000.0
    row = next(s for s in c.get("/api/live/sessions").json()["data"] if s["id"] == sid)
    assert float(row["live_capital"]) == 1000000.0

    # 未传 → None（用组合初始资金 = 现行为）
    sid2 = _create_session(c, portfolio_ids=(ps_id,))
    detail2 = c.get("/api/live/sessions/%d" % sid2).json()["data"]
    assert detail2["live_capital"] is None


def test_create_session_rejects_non_positive_live_capital(client, mock_bridge):
    """live_capital 提供（非 None）时须 >0：0 / 负数 / 非数值 → 400 业务错误。"""
    c, _ = client
    for bad in (0, -100, "abc"):
        resp = c.post("/api/live/sessions", json={
            "name": "bad-cap", "portfolio_ids": [], "live_capital": bad,
        })
        body = resp.json()
        assert body["code"] == 400
        assert "实盘资金基准须大于 0" in body["message"]


def test_build_engine_keeps_per_portfolio_capital(client, monkeypatch, caplog):
    """build_engine 的资金基准口径（1.0.5 起）：按组合占比切分，同时作真实侧总闸。

    新口径（2026-11-16/1.0.5 引入，`41c25b4`）：
    - ``live_capital`` 是**会话级真实账户总资产基准**（起账时 min(填写值, 真实总资产)，
      只缩不放）；
    - 起账时按各组合 ``initial_capital`` 占比切分虚拟现金（单组合 = 整额）；
    - 同一基准继续作为真实侧兜底总闸（引擎 ``_account_total_capital``，桥快照不可用
      时 ×0.98 − 在途预留）。

    旧口径「分账不分池、live_capital 只作真实侧总闸、组合恒按自身配置 10 万起账」
    自 1.0.5 起作废：live_capital 语义已改为会话真实资金基准，多组合必须共享同一
    基准并按配置占比切分，否则虚拟账本总和会超过真实资金（多组合超买）。

    一句话新口径：资金按各组合 initial_capital 权重从会话整仓基准切分，
    单组合场景等于整仓，故 500000 全部计入该组合。
    """
    import logging
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    real_cls = live_api.HttpBridgeDispatcher

    def fake_constructor(base_url="http://127.0.0.1:8790", **kw):
        client_ = httpx.Client(transport=httpx.MockTransport(
            lambda req: httpx.Response(200, json={"ok": True})))
        return real_cls(base_url=base_url, client=client_)

    monkeypatch.setattr(live_api, "HttpBridgeDispatcher", fake_constructor)

    sid = _create_session(c, portfolio_ids=(ps_id,), live_capital=500000)
    db2 = Session()
    try:
        with caplog.at_level(logging.INFO, logger="core.services.live_service"):
            engine = live_api._build_engine(sid, db2, "simulation")
        # 单组合 = 整额：会话基准 500000 全部作为该组合起账
        assert engine.portfolios[0].account.initial_capital == Decimal("500000.00")
        assert engine.portfolios[0].account.cash == Decimal("500000.00")
        # 同一基准仍是真实侧兜底总闸
        assert engine._account_total_capital == Decimal("500000")
        assert any("资金基准切分" in r.getMessage() for r in caplog.records)
        assert any("起账权重" in r.getMessage() for r in caplog.records)
    finally:
        db2.close()

    # 多组合：按 initial_capital 占比切分（100000:200000 → 1/3 : 2/3）
    db_extra = Session()
    try:
        ps2_id = _seed_extra_portfolio(db_extra)
    finally:
        db_extra.close()
    sid_multi = _create_session(c, portfolio_ids=(ps_id, ps2_id), live_capital=300000)
    db_multi = Session()
    try:
        engine_multi = live_api._build_engine(sid_multi, db_multi, "simulation")
        allocated = sorted(
            p.account.initial_capital for p in engine_multi.portfolios)
        assert allocated == [Decimal("100000.00"), Decimal("200000.00")]
        # 切分不得丢分：各组合起账之和严格等于会话基准
        assert sum(allocated) == Decimal("300000.00")
        assert engine_multi._account_total_capital == Decimal("300000")
    finally:
        db_multi.close()

    # 未设置 live_capital → 组合配置初始资金 100000（旧行为不变）
    sid2 = _create_session(c, portfolio_ids=(ps_id,))
    db3 = Session()
    try:
        engine2 = live_api._build_engine(sid2, db3, "simulation")
        assert engine2.portfolios[0].account.initial_capital == Decimal("100000")
    finally:
        db3.close()


def test_start_session_rejected_when_bridge_offline(client, monkeypatch):
    """桥未启动时 /start 返回 503 业务错误，不建引擎、不置 running。

    防回归：「一点启动就成功、桥离线却无告警」——启动前必须探测桥在线。
    """
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    real_cls = live_api.HttpBridgeDispatcher

    def fake_constructor(base_url="http://127.0.0.1:8790", **kw):
        # /ping 走 MockTransport 但返回 503 → heartbeat() 见 status_code != 200 返回 False
        client_ = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(503)))
        return real_cls(base_url=base_url, client=client_, channel=kw.get("channel"))

    monkeypatch.setattr(live_api, "HttpBridgeDispatcher", fake_constructor)
    monkeypatch.setattr(live_svc, "HttpBridgeDispatcher", fake_constructor)

    # 显式 engine=goldminer：验证 HTTP 桥离线时 503，不依赖全局默认引擎。
    sid = _create_session(c, portfolio_ids=(ps_id,), mode="simulation", engine="goldminer")
    resp = c.post("/api/live/sessions/%d/start" % sid)
    body = resp.json()
    assert body["code"] == 503
    assert "应答格式不符" in body["message"]
    # 未建引擎、session 仍非 running
    assert sid not in live_api._ENGINES
    detail = c.get("/api/live/sessions/%d" % sid).json()["data"]
    assert detail["status"] != "running"


# ---- §8.3 手动恢复熔断 ----

def _set_breaker(Session, sid, ps_id, count=3):
    """手动把 LiveSessionPortfolio 置为 circuit_broken + count。"""
    db = Session()
    try:
        link = db.query(LiveSessionPortfolio).filter_by(
            session_id=sid, portfolio_strategy_id=ps_id
        ).first()
        assert link is not None
        link.circuit_breaker_count = count
        link.status = "circuit_broken"
        db.commit()
    finally:
        db.close()


def test_recover_breaker_resets_running_session(client, mock_bridge):
    """运行中 session：POST recover → 引擎内存态 + DB 双写解除熔断。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    sid = _create_session(c, portfolio_ids=(ps_id,))
    c.post("/api/live/sessions/%d/start" % sid)
    engine = live_api._ENGINES[sid]

    # 手动置 DB + 引擎内存态为熔断
    _set_breaker(Session, sid, ps_id, count=3)
    port = next(p for p in engine.portfolios if p.portfolio_id == ps_id)
    rm = port.risk_manager
    rm.consecutive_drawdown_triggers = 3
    rm.manual_recovery = True
    rm.circuit_breaker_active = True
    engine._breaker_count_written[ps_id] = 3

    resp = c.post("/api/live/sessions/%d/portfolios/%d/recover" % (sid, ps_id),
                  json={"confirm": True, "reason": "人工确认恢复"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    assert body["data"]["status"] == "active"
    assert body["data"]["circuit_breaker_count"] == 0
    assert body["data"]["reason"] == "人工确认恢复"
    # 内存态
    assert rm.manual_recovery is False
    assert rm.circuit_breaker_active is False
    assert rm.consecutive_drawdown_triggers == 0
    assert rm.is_trading_halted() is False
    assert engine._breaker_count_written[ps_id] == 0
    # DB 态
    db = Session()
    try:
        link = db.query(LiveSessionPortfolio).filter_by(
            session_id=sid, portfolio_strategy_id=ps_id
        ).first()
        assert link.status == "active"
        assert link.circuit_breaker_count == 0
        # D5 审计：恢复原因写入决策事件
        evt = db.query(LiveDecisionEvent).filter_by(
            live_session_id=sid, gate="breaker_recover").first()
        assert evt is not None
        assert "人工确认恢复" in (evt.message or "")
        assert "portfolio=%d" % ps_id in (evt.message or "")
    finally:
        db.close()

    c.post("/api/live/sessions/%d/stop" % sid)


def test_recover_breaker_stopped_session_only_db(client, mock_bridge):
    """未运行 session：POST recover → 只改 DB（引擎未运行，无内存态可重置）。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    sid = _create_session(c, portfolio_ids=(ps_id,))
    assert sid not in live_api._ENGINES  # 未 start
    _set_breaker(Session, sid, ps_id, count=3)

    resp = c.post("/api/live/sessions/%d/portfolios/%d/recover" % (sid, ps_id),
                  json={"confirm": True, "reason": "收盘后人工复核"})

    assert resp.status_code == 200
    assert resp.json()["code"] == 0
    assert resp.json()["data"]["status"] == "active"
    assert resp.json()["data"]["circuit_breaker_count"] == 0
    db = Session()
    try:
        link = db.query(LiveSessionPortfolio).filter_by(
            session_id=sid, portfolio_strategy_id=ps_id
        ).first()
        assert link.status == "active"
        assert link.circuit_breaker_count == 0
        evt = db.query(LiveDecisionEvent).filter_by(
            live_session_id=sid, gate="breaker_recover").first()
        assert evt is not None and "收盘后人工复核" in (evt.message or "")
    finally:
        db.close()


def test_recover_breaker_manual_recovery_requires_confirm(client, mock_bridge):
    """D5：count>=3 已转手动恢复时，未带 confirm 的恢复被拒（400），状态不变。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    sid = _create_session(c, portfolio_ids=(ps_id,))
    _set_breaker(Session, sid, ps_id, count=3)

    resp = c.post("/api/live/sessions/%d/portfolios/%d/recover" % (sid, ps_id))
    assert resp.status_code == 200
    assert resp.json()["code"] == 400
    assert "二次确认" in resp.json()["message"]

    db = Session()
    try:
        link = db.query(LiveSessionPortfolio).filter_by(
            session_id=sid, portfolio_strategy_id=ps_id
        ).first()
        assert link.status == "circuit_broken"
        assert link.circuit_breaker_count == 3
        assert db.query(LiveDecisionEvent).filter_by(
            live_session_id=sid, gate="breaker_recover").first() is None
    finally:
        db.close()


def test_recover_breaker_session_not_found(client, mock_bridge):
    """session 不存在 → 404。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    resp = c.post("/api/live/sessions/9999/portfolios/%d/recover" % ps_id)

    assert resp.status_code == 200
    assert resp.json()["code"] == 404


def test_recover_breaker_portfolio_not_in_session(client, mock_bridge):
    """组合不在该 session → 404。"""
    c, Session = client
    db = Session()
    ps_id = _seed(db)
    db.close()

    sid = _create_session(c, portfolio_ids=(ps_id,))

    resp = c.post("/api/live/sessions/%d/portfolios/8888/recover" % sid)

    assert resp.status_code == 200
    assert resp.json()["code"] == 404


# ---------------- C1（终审）：start check-then-act 竞态闭合 ----------------

def _conc_session_factory(tmp_path):
    """并发 start 用文件 SQLite 工厂：两个并发请求各持独立连接（fixture 的 StaticPool
    单连接不允许跨 session 并发事务），WAL 降低读写锁冲突。"""
    from sqlalchemy import event as sa_event

    eng = create_engine(
        "sqlite:///%s" % (tmp_path / "conc-start.db"),
        connect_args={"check_same_thread": False, "timeout": 30},
    )

    @sa_event.listens_for(eng, "connect")
    def _pragma(dbapi_conn, _rec):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA busy_timeout=30000")
        cur.close()

    Base.metadata.create_all(eng)
    return sessionmaker(bind=eng, expire_on_commit=False)


def test_concurrent_start_same_session_registers_single_engine(
        client, mock_bridge, monkeypatch, tmp_path):
    """C1：并发两次 start 同一 session（threadpool 启动窗口内重叠）→ 仅一个引擎注册，
    svc.start_session 只被调用一次，第二次得幂等 running（不产生双引擎并发下单）。"""
    import asyncio
    import threading

    import core.services.live_service as live_svc

    ConcSession = _conc_session_factory(tmp_path)
    # 独立连接工厂供引擎组装/recover 使用（替代 fixture 的 StaticPool 单连接）
    monkeypatch.setattr(live_api, "SessionLocal", ConcSession)

    from core.models import LiveSession as _LiveSession

    db = ConcSession()
    ps_id = _seed(db)
    session = _LiveSession(
        name="conc-start", mode="simulation", status="stopped",
        engine="goldminer", execution_mode="auto",
    )
    db.add(session)
    db.flush()
    db.add(LiveSessionPortfolio(session_id=session.id, portfolio_strategy_id=ps_id))
    db.commit()
    sid = session.id
    db.close()

    real_start = live_svc.start_session
    entered = threading.Event()
    release = threading.Event()
    calls = {"n": 0}

    def slow_start(*args, **kwargs):
        # 复刻竞态窗口：svc.start_session 在 threadpool 里秒级阻塞（heartbeat/recover）
        calls["n"] += 1
        entered.set()
        release.wait(timeout=10)
        return real_start(*args, **kwargs)

    monkeypatch.setattr(live_svc, "start_session", slow_start)

    async def call_start(session_factory):
        db_req = session_factory()
        try:
            # 路由签名 1.0.5 起为 (session_id, data=None, db=Depends)；必须用关键字
            # 传 db，否则 Session 会被当成 data（data.get → TypeError）。
            return await live_api.start_session(sid, data=None, db=db_req)
        finally:
            db_req.close()

    async def drive():
        t1 = asyncio.create_task(call_start(ConcSession))
        while not entered.is_set():
            if t1.done():
                # 启动协程提前失败（如线程池/锁不可用）时，直接抛出原因，
                # 避免 while 空转把整个测试挂死。
                t1.result()
            await asyncio.sleep(0.01)
        # 第一个 start 仍在 threadpool 窗口内（未注册）→ 并发发起第二个
        t2 = asyncio.create_task(call_start(ConcSession))
        await asyncio.sleep(0.2)
        # C1 断言：锁生效——第二个 start 在锁外等待，未进入启动临界区
        assert calls["n"] == 1, (
            "并发第二个 start 进入了启动临界区（_START_LOCK 未生效）"
        )
        release.set()
        r1, r2 = await asyncio.gather(t1, t2)
        engine = live_api._ENGINES.get(sid)
        await asyncio.sleep(0.05)  # 让 create_task(engine.start()) 推进到可停状态
        if engine is not None:
            await engine.stop()
        return r1, r2

    r1, r2 = asyncio.run(drive())

    assert r1["code"] == 0 and r1["data"]["status"] == "running"
    assert r2["code"] == 0 and r2["data"]["status"] == "running"  # 幂等语义
    assert calls["n"] == 1  # 第二次 start 走幂等路径，svc.start_session 仅调用一次
    assert list(live_api._ENGINES) == [sid]  # 仅注册一个引擎（无双引擎并发下单）

