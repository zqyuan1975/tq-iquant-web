import logging
import time
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from core.services import live_service as svc


@pytest.fixture(autouse=True)
def _clear_probe_cache():
    """每个用例独立：清空通道探活 TTL 缓存，避免跨用例复用上次结果。"""
    svc._PROBE_CACHE.clear()
    yield
    svc._PROBE_CACHE.clear()


def _cfg(ths_enabled=False, tq_trade=None):
    cfg = {
        "iquant_bridge": {
            "simulation": {"base_url": "http://127.0.0.1:8790"},
            "live": {"base_url": "http://127.0.0.1:8791"},
        },
        "ths_ui": {
            "enabled": ths_enabled,
            "window_title": "网上股票交易系统5.0",
            "client_exe_path": "D:/同花顺远航版/transaction/xiadan.exe",
            "driver": "clipboard",
        },
    }
    if tq_trade:
        cfg["tq_trade"] = tq_trade
    return cfg


def test_channel_defaults_to_configured_default_engine(monkeypatch):
    """无 session.engine 时按 default_engine（tq_trade.default_engine=tdx）。"""
    monkeypatch.setattr(
        svc, "load_config", lambda: _cfg(False, tq_trade={"default_engine": "tdx"}))
    data = svc.channel_status()
    assert data["active"] == "tdx_tq"
    assert data["label"].startswith("通达信")
    assert data["online"] is None
    assert data["bridge"]["simulation"] == "http://127.0.0.1:8790"


def test_channel_defaults_to_legacy_iquant_when_no_config(monkeypatch):
    """未配置 default_engine 且 ths 未启用 → 旧行为回退 iquant（桥已恢复接入）。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    data = svc.channel_status()
    assert data["engine"] == "iquant"
    assert data["active"] == "iquant_bridge"
    assert data["label"].startswith("QMT 通道")


def test_channel_switches_to_ths_when_enabled_and_available(monkeypatch):
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(True))
    monkeypatch.setattr(svc, "ThsUiDispatcher", object)
    data = svc.channel_status()
    assert data["active"] == "ths_ui"
    assert data["ths_ui"]["enabled"] is True
    assert data["ths_ui"]["available"] is True
    assert data["ths_ui"]["window_title"] == "网上股票交易系统5.0"


def test_channel_falls_back_when_dependency_missing(monkeypatch):
    """THS 依赖缺失 → 回退 default_engine（tdx）。"""
    monkeypatch.setattr(
        svc, "load_config", lambda: _cfg(True, tq_trade={"default_engine": "tdx"}))
    monkeypatch.setattr(svc, "ThsUiDispatcher", None)
    data = svc.channel_status()
    assert data["active"] == "tdx_tq"
    assert data["ths_ui"]["enabled"] is True
    assert data["ths_ui"]["available"] is False


def test_test_channel_ths_without_dependency(monkeypatch):
    monkeypatch.setattr(svc, "ThsUiDispatcher", None)
    out = svc.test_channel("ths_ui")
    assert out["ok"] is False
    assert "pywinauto" in out["error"]


def test_test_channel_bridge_ping(monkeypatch):
    class FakeBridge:
        def __init__(self, base_url=None):
            self.base_url = base_url

        def heartbeat(self):
            return True

    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    monkeypatch.setattr(svc, "HttpBridgeDispatcher", FakeBridge)
    out = svc.test_channel("iquant_bridge", "live")
    assert out == {"ok": True, "channel": "iquant_bridge",
                   "base_url": "http://127.0.0.1:8791", "error": None}


def test_test_channel_ths_requires_captcha_ocr(monkeypatch):
    cfg = _cfg(True)
    cfg["ths_ui"]["captcha_ocr"] = False
    monkeypatch.setattr(svc, "load_config", lambda: cfg)

    class FakeThs:
        channel = "ths_ui"
        last_error = ""

        def __init__(self, **kwargs):
            pass

        def connect(self):
            return True

        def query_positions(self):
            return []

        def close(self):
            pass

    monkeypatch.setattr(svc, "ThsUiDispatcher", FakeThs)
    out = svc.test_channel("ths_ui")
    assert out["ok"] is False
    assert "验证码 OCR" in out["error"]


def test_ths_dispatcher_reports_channel_and_error(monkeypatch):
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(True))
    seen = {}

    class FakeThs:
        channel = "ths_ui"
        last_error = ""

        def __init__(self, window_title=None, client_exe_path=None, captcha_ocr=False,
                     execute_mode=None):
            seen["window_title"] = window_title
            seen["client_exe_path"] = client_exe_path
            seen["captcha_ocr"] = captcha_ocr
            seen["execute_mode"] = execute_mode

        def connect(self):
            return False

        def query_positions(self):
            return []

        def close(self):
            pass

    monkeypatch.setattr(svc, "ThsUiDispatcher", FakeThs)
    out = svc.test_channel("ths_ui")
    assert out["ok"] is False
    assert seen["window_title"] == "网上股票交易系统5.0"
    assert seen["client_exe_path"].endswith("xiadan.exe")
    # 只读探测不得启文件交接执行器（否则 data/ths/0 会挂消费线程）
    assert seen["execute_mode"] == "direct"


# ---------------------------------------------------------------- 四引擎可选

def test_engine_status_lists_four_engines(monkeypatch):
    """引擎清单（QMT 恢复接入）：掘金/通达信/同花顺/QMT 通道。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    data = svc.engine_status("goldminer")
    assert [e["key"] for e in data["engines"]] == [
        "goldminer", "tdx", "ths", "iquant"]
    assert data["engine"] == "goldminer"
    assert data["active"] == "goldminer_bridge"
    assert data["engines"][-1]["key"] == "iquant"


def test_engine_status_iquant_channel_and_detail(monkeypatch):
    """QMT 项：通道键 iquant_bridge，detail 带仿真/实盘双桥地址。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    data = svc.engine_status("iquant")
    item = {e["key"]: e for e in data["engines"]}["iquant"]
    assert item["channel"] == "iquant_bridge"
    assert item["label"] == "QMT 通道"
    assert item["configured"] is True
    assert "http://127.0.0.1:8790" in item["detail"]
    assert "http://127.0.0.1:8791" in item["detail"]
    assert data["active"] == "iquant_bridge"


def test_channel_status_uses_session_engine(monkeypatch):
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    data = svc.channel_status(engine_key="tdx", engines={})
    assert data["engine"] == "tdx"
    assert data["active"] == "tdx_tq"
    assert data["label"].startswith("通达信")


def test_channel_tdx_no_engine_probes_client_window(monkeypatch):
    """引擎未运行也要真实探测：tdx 看客户端窗口，不在 → 离线（不默绿）。

    探针只做窗口存在性判断，不构造 TdxTqDispatcher（TQ 调用会冻结客户端）。
    """
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    monkeypatch.setattr(svc, "_window_present", lambda key: False)

    class _Boom:
        def __init__(self, **kw):
            raise AssertionError("窗口探针不得构造 dispatcher")

    monkeypatch.setattr(svc, "TdxTqDispatcher", _Boom)
    svc._PROBE_CACHE.clear()
    data = svc.channel_status(engine_key="tdx", engines={})
    assert data["online"] is False
    assert data["online_source"] == "probe"
    assert data["engine_running"] is False


def test_channel_ths_no_engine_probes_client_window(monkeypatch):
    """同花顺：xiadan/hexin 窗口在 → 在线；同一套 online 布尔语义。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    monkeypatch.setattr(svc, "_window_present", lambda key: key == "ths")
    svc._PROBE_CACHE.clear()
    data = svc.channel_status(engine_key="ths", engines={})
    assert data["online"] is True
    assert data["online_source"] == "probe"
    svc._PROBE_CACHE.clear()
    monkeypatch.setattr(svc, "_window_present", lambda key: False)
    data = svc.channel_status(engine_key="ths", engines={})
    assert data["online"] is False


def test_channel_running_engine_reports_real_heartbeat(monkeypatch):
    """无引擎状态替身 → 回退探活（30s TTL 缓存内复用，清缓存后如实反映离线）。"""
    import types

    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    engine = types.SimpleNamespace(
        dispatcher=types.SimpleNamespace(channel="tdx_tq", heartbeat=lambda: True))
    data = svc.channel_status(engine_key="tdx", engines={1: engine})
    assert data["engine_running"] is True
    assert data["online"] is True

    engine.dispatcher.heartbeat = lambda: False
    svc._PROBE_CACHE.clear()   # TTL 缓存内会复用上次结果，清空后重新探活
    data = svc.channel_status(engine_key="tdx", engines={1: engine})
    assert data["engine_running"] is True
    assert data["online"] is False


def test_channel_running_goldminer_engine_reports_running(monkeypatch):
    """掘金通道也必须在映射表内：有运行引擎 → engine_running=True + 真实心跳。

    缺陷（2026-11-05）：`_ENGINE_CHANNEL` 只覆盖 tdx/ths/iquant，漏了 goldminer，
    会话 #3（engine=goldminer）实际 running 时仍返回 engine_running=False、
    online=None，前端通道卡片误显示「未检测」。
    """
    import types

    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    engine = types.SimpleNamespace(
        dispatcher=types.SimpleNamespace(
            channel="goldminer_bridge", heartbeat=lambda: True))
    data = svc.channel_status(engine_key="goldminer", engines={3: engine})
    assert data["active"] == "goldminer_bridge"
    assert data["engine_running"] is True
    assert data["online"] is True   # 不得出现 None 的「未检测」态


def test_channel_running_iquant_engine_reports_running(monkeypatch):
    """QMT 恢复后同通道映射必须生效：运行中 → engine_running=True。"""
    import types

    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    engine = types.SimpleNamespace(
        bridge_online=True,
        dispatcher=types.SimpleNamespace(
            channel="iquant_bridge", heartbeat=lambda: True))
    data = svc.channel_status(engine_key="iquant", engines={5: engine})
    assert data["active"] == "iquant_bridge"
    assert data["engine_running"] is True
    assert data["online"] is True


def test_channel_goldminer_no_running_engine_probes_bridge(monkeypatch):
    """无掘金引擎在跑时也探 7801 桥：不可达 → 离线（不再有「未检测」第三态）。"""
    import types

    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    seen = {}

    class FakeBridge:
        def __init__(self, base_url=None, **kw):
            seen["base_url"] = base_url

        def heartbeat(self):
            return False

    monkeypatch.setattr(svc, "HttpBridgeDispatcher", FakeBridge)
    tdx = types.SimpleNamespace(
        dispatcher=types.SimpleNamespace(channel="tdx_tq", heartbeat=lambda: True))
    svc._PROBE_CACHE.clear()
    data = svc.channel_status(engine_key="goldminer", engines={1: tdx})
    assert data["engine_running"] is False
    assert data["online"] is False
    assert data["online_source"] == "probe"
    assert seen["base_url"] == "http://127.0.0.1:7801"


def test_channel_iquant_without_engine_probes_production_bridge(monkeypatch):
    """无运行引擎（会话未启动）时 QMT 仍按常驻桥探活：桥可达 → 在线。

    QMT 桥是常驻 HTTP 服务，可达性与会话是否启动无关；卡片必须真实反映桥
    是否可达，不能因为「没有引擎」就一律显示未检测。
    """
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    seen = {}

    class FakeBridge:
        def __init__(self, base_url=None, **kw):
            seen["base_url"] = base_url

        def heartbeat(self):
            return True

    monkeypatch.setattr(svc, "HttpBridgeDispatcher", FakeBridge)
    svc._PROBE_CACHE.clear()
    data = svc.channel_status(engine_key="iquant", engines={})
    assert seen["base_url"] == "http://127.0.0.1:8790"   # 只探生产仿真桥
    assert data["engine_running"] is False
    assert data["online"] is True
    assert data["online_source"] == "probe"


def test_channel_iquant_bridge_down_reports_offline_not_green(monkeypatch):
    """桥没运行 → online=False（离线/异常），绝不默认亮绿，也不退回 None 未检测。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))

    class FakeBridge:
        def __init__(self, base_url=None, **kw):
            pass

        def heartbeat(self):
            return False

    monkeypatch.setattr(svc, "HttpBridgeDispatcher", FakeBridge)
    svc._PROBE_CACHE.clear()
    data = svc.channel_status(engine_key="iquant", engines={})
    assert data["online"] is False
    assert data["online"] is not None
    assert data["online_source"] == "probe"


def test_channel_iquant_probe_exception_reports_offline(monkeypatch):
    """探活自身异常（依赖缺失/构造失败）同样判离线，不得抛到页面请求。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))

    class BoomBridge:
        def __init__(self, **kw):
            raise OSError("connect failed")

    monkeypatch.setattr(svc, "HttpBridgeDispatcher", BoomBridge)
    data = svc.channel_status(engine_key="iquant", engines={})
    assert data["online"] is False
    assert data["online_source"] == "probe"


def test_channel_detail_quant_reports_8790_only(monkeypatch):
    """QMT 卡片次要信息只报生产 8790 仿真桥；8791 信用腿非生产未启用，不得出现。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    monkeypatch.setattr(svc, "HttpBridgeDispatcher", None)   # 不探活，避免真实网络
    data = svc.channel_status(engine_key="iquant", engines={})
    assert "http://127.0.0.1:8790" in data["detail"]
    assert "8791" not in data["detail"]
    assert data["configured"] is False   # 依赖缺失 → 未配置
    assert data["online"] is False       # 探不到 → 离线，不臆造在线


def test_channel_detail_present_and_consistent_for_all_four(monkeypatch):
    """四通道同一套字段：configured 是布尔、detail 非空（端口/账号/窗口）。"""
    monkeypatch.setattr(
        svc, "load_config", lambda: _cfg(False, tq_trade={"account": "A1"}))
    monkeypatch.setattr(svc, "HttpBridgeDispatcher", None)
    monkeypatch.setattr(svc, "_window_present", lambda key: False)
    details = {}
    for key in ("tdx", "ths", "goldminer", "iquant"):
        data = svc.channel_status(engine_key=key, engines={})
        assert isinstance(data["configured"], bool)
        assert data["detail"], key
        assert isinstance(data["online"], bool)   # 四通道恒为布尔，无第三态
        details[key] = data["detail"]
    assert "A1" in details["tdx"]                       # 资金账号
    assert details["ths"]                               # 窗口标题
    assert details["goldminer"].startswith("http://")   # 掘金桥地址
    assert details["iquant"].startswith("HTTP 桥 http://")


def test_channel_ignores_engines_of_other_channels(monkeypatch):
    """注册表里是别的通道（如 ths）时，tdx 仍应按「未运行」处理。"""
    import types

    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    monkeypatch.setattr(svc, "_window_present", lambda key: False)
    ths = types.SimpleNamespace(
        dispatcher=types.SimpleNamespace(channel="ths_ui", heartbeat=lambda: True))
    svc._PROBE_CACHE.clear()
    data = svc.channel_status(engine_key="tdx", engines={7: ths})
    assert data["engine_running"] is False
    assert data["online"] is False   # 别的通道的引擎不算数，按本通道真实探测判离线


def test_channel_status_prefers_engine_state_without_probe(monkeypatch):
    """有运行引擎 → 只读引擎最近已知在线态，绝不新发 heartbeat（慢桥不阻塞页面）。

    实证 2026-11-05：掘金桥 /ping 耗时 7.7s（桥内全局锁串行），状态总览整页卡住。
    """
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    probed = []
    engine = SimpleNamespace(
        bridge_online=True,
        _last_tick_at=datetime.now(),
        dispatcher=SimpleNamespace(
            channel="goldminer_bridge", heartbeat=lambda: probed.append(1) or True))
    data = svc.channel_status(engine_key="goldminer", engines={3: engine})
    assert data["engine_running"] is True
    assert data["online"] is True
    assert data["online_source"] == "engine"
    assert data["stale"] is False
    assert data["checked_at"] is not None
    assert probed == []   # 关键：页面请求未再打桥


def test_channel_status_engine_state_stale_when_tick_old(monkeypatch):
    """引擎主循环长时间未 tick（> max(180s, 3×poll)）→ 状态标 stale。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    engine = SimpleNamespace(
        bridge_online=False,
        _last_tick_at=datetime.now() - timedelta(seconds=400),
        _poll_interval=60,
        dispatcher=SimpleNamespace(channel="goldminer_bridge", heartbeat=lambda: True))
    data = svc.channel_status(engine_key="goldminer", engines={3: engine})
    assert data["online"] is False
    assert data["stale"] is True
    assert data["checked_at"] is not None


def test_channel_probe_times_out_within_one_second(monkeypatch):
    """无引擎状态的慢心跳：1s 硬超时返回（判离线 + stale），不拖住 HTTP 请求。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))

    class SlowDispatcher:
        channel = "tdx_tq"

        def heartbeat(self):
            time.sleep(5)
            return True

    engine = SimpleNamespace(dispatcher=SlowDispatcher())
    t0 = time.monotonic()
    data = svc.channel_status(engine_key="tdx", engines={1: engine})
    elapsed = time.monotonic() - t0
    assert elapsed < 1.5
    assert data["online"] is False
    assert data["stale"] is True
    assert data["online_source"] == "probe"


def test_channel_probe_reuses_ttl_cache(monkeypatch):
    """探活结果 30s 内复用：连续两次 channel_status 只打一次心跳。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    probed = []

    class CountingDispatcher:
        channel = "tdx_tq"

        def heartbeat(self):
            probed.append(1)
            return True

    engine = SimpleNamespace(dispatcher=CountingDispatcher())
    svc.channel_status(engine_key="tdx", engines={1: engine})
    svc.channel_status(engine_key="tdx", engines={1: engine})
    assert len(probed) == 1


def test_bridge_status_reads_engine_state_without_probe():
    """session 详情/bridge-status 也走引擎已知态，不新发 /ping。"""
    def _boom():
        raise AssertionError("不应在页面请求里探活")

    engine = SimpleNamespace(
        bridge_online=False,
        dispatcher=SimpleNamespace(heartbeat=_boom))
    assert svc.bridge_status(1, "running", engine) == {"online": False, "status": "running"}


def test_make_dispatcher_by_engine(monkeypatch):
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(
        False,
        tq_trade={"account": "A1", "account_type": "stock", "price_mode": "latest"},
    ))
    seen = {}

    class FakeTdx:
        def __init__(self, **kw):
            seen["tdx"] = kw

    monkeypatch.setattr(svc, "TdxTqDispatcher", FakeTdx)
    assert isinstance(svc.make_dispatcher("tdx"), FakeTdx)
    assert seen["tdx"]["account"] == "A1"
    assert seen["tdx"]["price_mode"] == "latest"


def test_make_dispatcher_raises_when_dependency_missing(monkeypatch):
    import pytest

    monkeypatch.setattr(svc, "TdxTqDispatcher", None)
    with pytest.raises(RuntimeError):
        svc.make_dispatcher("tdx")


def test_make_dispatcher_iquant_picks_bridge_by_mode(monkeypatch):
    """iquant：simulation→8790 / live→8791 / 未知 mode 兜底仿真桥。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    seen = {}

    class FakeBridge:
        def __init__(self, **kw):
            seen.update(kw)

    monkeypatch.setattr(svc, "HttpBridgeDispatcher", FakeBridge)

    svc.make_dispatcher("iquant", "simulation", 1)
    assert seen["base_url"] == "http://127.0.0.1:8790"
    assert seen["channel"] == "iquant_bridge"

    svc.make_dispatcher("iquant", "live", 2)
    assert seen["base_url"] == "http://127.0.0.1:8791"

    # 未知 mode 不得落到真实资金桥（8791），兜底仿真桥
    svc.make_dispatcher("iquant", "unknown", 3)
    assert seen["base_url"] == "http://127.0.0.1:8790"


def test_make_dispatcher_iquant_reads_timeouts_from_config(monkeypatch):
    """超时可配置：config.iquant_bridge 三键生效；缺省/非法回退合理默认。"""
    cfg = _cfg(False)
    cfg["iquant_bridge"].update({
        "request_timeout_seconds": "12",
        "slow_response_seconds": 6,
        "slow_warn_interval_seconds": 120,
    })
    monkeypatch.setattr(svc, "load_config", lambda: cfg)
    seen = {}

    class FakeBridge:
        def __init__(self, **kw):
            seen.update(kw)

    monkeypatch.setattr(svc, "HttpBridgeDispatcher", FakeBridge)
    svc.make_dispatcher("iquant", "simulation")
    assert seen["timeout"] == 12.0
    assert seen["slow_threshold"] == 6.0
    assert seen["slow_warn_interval"] == 120.0

    # 未配置 → 默认（本机 loopback 桥：15s 读超时 / 5s 慢阈值 / 300s 冷却窗）
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    svc.make_dispatcher("iquant", "simulation")
    assert seen["timeout"] == 15.0
    assert seen["slow_threshold"] == 5.0
    assert seen["slow_warn_interval"] == 300.0


def test_make_dispatcher_iquant_raises_when_dependency_missing(monkeypatch):
    import pytest

    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    monkeypatch.setattr(svc, "HttpBridgeDispatcher", None)
    with pytest.raises(RuntimeError):
        svc.make_dispatcher("iquant")


def test_bridge_probe_message_labels_iquant(monkeypatch):
    """QMT 通道的启动闸提示不能笼统写「桥」，要能区分是哪座桥。"""
    msg = svc.bridge_probe_message("iquant_bridge", "refused", "http://127.0.0.1:8790")
    assert "QMT 通道" in msg
    assert "端口无监听" in msg


def test_test_channel_tdx_reports_not_ready(monkeypatch):
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))

    class FakeTdx:
        channel = "tdx_tq"
        last_error = "请启动客户端"

        def __init__(self, **kw):
            pass

        def heartbeat(self):
            return False

        def query_positions(self):
            return []

        def close(self):
            pass

    monkeypatch.setattr(svc, "TdxTqDispatcher", FakeTdx)
    out = svc.test_channel("tdx_tq")
    assert out["ok"] is False
    assert out["channel"] == "tdx_tq"
    assert "请启动客户端" in out["error"]


# ---------------- 持仓计数：0 股残行不算持仓 ----------------

def test_real_position_count_ignores_zero_volume():
    """通达信卖出后持仓表保留 0 股残行 → 3 行里只有 1 条真实持仓。"""
    assert svc.real_position_count([
        {"volume": 0, "available": 0},
        {"volume": 0, "available": 0},
        {"volume": 300, "available": 300},
    ]) == 1


def test_real_position_count_keeps_t1_locked_position():
    """当日买入 T+1 锁仓：available=0 但 volume>0 仍是真实持仓。"""
    assert svc.real_position_count([
        {"volume": 300, "available": 0},
        {"volume": 0, "available": 0},
    ]) == 1


def test_test_channel_tdx_counts_real_positions_only(monkeypatch):
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))

    class FakeTdx:
        channel = "tdx_tq"
        last_error = ""

        def __init__(self, **kw):
            pass

        def heartbeat(self):
            return True

        def query_positions(self):
            return [
                {"instrument": "002056", "volume": 0, "available": 0},
                {"instrument": "600888", "volume": 0, "available": 0},
                {"instrument": "600000", "volume": 300, "available": 300},
            ]

        def close(self):
            pass

    monkeypatch.setattr(svc, "TdxTqDispatcher", FakeTdx)
    out = svc.test_channel("tdx_tq")
    assert out["ok"] is True
    assert out["positions"] == 1


def test_test_channel_ths_counts_real_positions_only(monkeypatch):
    cfg = _cfg(True)
    cfg["ths_ui"]["captcha_ocr"] = True
    monkeypatch.setattr(svc, "load_config", lambda: cfg)

    class FakeThs:
        channel = "ths_ui"
        last_error = ""

        def __init__(self, **kwargs):
            pass

        def connect(self):
            return True

        def query_positions(self):
            return [
                {"instrument": "002056", "volume": 0, "available": 0},
                {"instrument": "600888", "volume": 0, "available": 0},
                {"instrument": "600000", "volume": 300, "available": 0},
            ]

        def close(self):
            pass

    monkeypatch.setattr(svc, "ThsUiDispatcher", FakeThs)
    out = svc.test_channel("ths_ui")
    assert out["ok"] is True
    assert out["positions"] == 1


# ---------------- 重放聚合：超卖流水钳制为 0、不展示负数 ----------------

def _replay_trade(qty, ttype, code="600888.SH", session_id=1):
    return SimpleNamespace(
        live_session_id=session_id, portfolio_strategy_id=1, strategy_id=1,
        stock_code=code, trade_type=ttype, price=10.0, quantity=qty,
        amount=float(qty) * 10.0, commission=0.0, stamp_duty=0.0,
        trade_time=datetime(2026, 11, 5, 9, 51),
    )


def test_replay_full_close_is_zero_not_negative():
    """买入 300 → 卖出 300：结果为 0（不展示行），不出现 -300。"""
    rows = svc.aggregate_positions_from_trades(
        [_replay_trade(300, "buy"), _replay_trade(300, "sell")])
    assert rows == []


def test_replay_sell_only_is_clamped_and_warned(caplog):
    """无期初/买入流水只有卖出（脏数据）：钳制为 0 + 日志告警。"""
    svc._OVERSELL_WARNED.clear()
    with caplog.at_level(logging.WARNING):
        rows = svc.aggregate_positions_from_trades([_replay_trade(300, "sell")])
    assert rows == []
    assert any("超卖流水" in r.getMessage() for r in caplog.records)


def test_create_session_persists_engine(db_session):
    data = svc.create_session(db_session, {"name": "s1", "engine": "tdx"}, "simulation")
    assert data["engine"] == "tdx"
    listed = svc.list_sessions(db_session)
    assert listed[0]["engine"] == "tdx"


# ---------------- 外部信号注入：signal_file_path 会话配置（0011） ----------------

def test_create_session_persists_signal_file_path(db_session):
    """create_session 透传存储 signal_file_path；get/list 序列化可见。"""
    data = svc.create_session(
        db_session, {"name": "ext", "engine": "tdx",
                     "signal_file_path": "D:/ths/alert.txt"}, "simulation")
    row = db_session.query(svc.LiveSession).filter_by(id=data["id"]).first()
    assert row.signal_file_path == "D:/ths/alert.txt"
    detail = svc.get_session(db_session, data["id"])
    assert detail["signal_file_path"] == "D:/ths/alert.txt"
    listed = svc.list_sessions(db_session)
    assert listed[0]["signal_file_path"] == "D:/ths/alert.txt"


def test_create_session_empty_signal_file_path_becomes_none(db_session):
    """空串/缺省 → None（不启用外部信号注入）。"""
    data = svc.create_session(
        db_session, {"name": "ext2", "engine": "tdx",
                     "signal_file_path": "   "}, "simulation")
    row = db_session.query(svc.LiveSession).filter_by(id=data["id"]).first()
    assert row.signal_file_path is None
    data2 = svc.create_session(db_session, {"name": "ext3", "engine": "tdx"}, "simulation")
    row2 = db_session.query(svc.LiveSession).filter_by(id=data2["id"]).first()
    assert row2.signal_file_path is None


# ---------------- 14:50 提前收盘价：advance_close_* 会话配置（功能二） ----------------

def test_create_session_persists_advance_close(db_session):
    """create_session 透传提前收盘价开关/时点；get/list 序列化可见。"""
    data = svc.create_session(
        db_session, {"name": "adv", "engine": "tdx",
                     "advance_close_enabled": True,
                     "advance_close_time": "14:55"}, "simulation")
    row = db_session.query(svc.LiveSession).filter_by(id=data["id"]).first()
    assert row.advance_close_enabled is True
    assert row.advance_close_time == "14:55"
    detail = svc.get_session(db_session, data["id"])
    assert detail["advance_close_enabled"] is True
    assert detail["advance_close_time"] == "14:55"
    assert detail["advance_close_done_date"] is None
    listed = svc.list_sessions(db_session)
    assert listed[0]["advance_close_enabled"] is True


def test_create_session_advance_close_defaults_and_clamp(db_session):
    """缺省 → 关 + 14:51；时点越界（早于 14:51 / 晚于 14:59）回退 14:51。"""
    data = svc.create_session(
        db_session, {"name": "adv2", "engine": "tdx"}, "simulation")
    row = db_session.query(svc.LiveSession).filter_by(id=data["id"]).first()
    assert row.advance_close_enabled is False
    assert row.advance_close_time == "14:51"
    data2 = svc.create_session(
        db_session, {"name": "adv3", "engine": "tdx",
                     "advance_close_time": "09:35"}, "simulation")
    row2 = db_session.query(svc.LiveSession).filter_by(id=data2["id"]).first()
    assert row2.advance_close_time == "14:51"


# ---------------- A-6：create_session 默认引擎（tq_trade.default_engine 覆盖） ----------------

def test_default_engine_prefers_tq_trade_override(monkeypatch):
    """A-6：tq_trade.default_engine 优先（用户主通道 tdx）。"""
    monkeypatch.setattr(
        svc, "load_config", lambda: _cfg(False, tq_trade={"default_engine": "tdx"}))
    assert svc.default_engine() == "tdx"


def test_session_engine_qmt_falls_back_to_default(db_session, monkeypatch):
    """历史会话兼容：qmt 通道已移除，显式 'qmt' 不再进白名单，
    session_engine 回退全局默认（tq_trade.default_engine），不加迁移。"""
    monkeypatch.setattr(
        svc, "load_config", lambda: _cfg(False, tq_trade={"default_engine": "tdx"}))
    from core.models import LiveSession
    session = LiveSession(
        name="legacy_qmt", mode="live", status="stopped", engine="qmt")
    db_session.add(session)
    db_session.commit()
    assert svc.session_engine(session) == "tdx"


def test_bridge_probe_message_three_tiers():
    """启动闸三档提示可区分：端口无监听 / 已监听但无响应 / 协议不符。"""
    url = "http://127.0.0.1:7801"
    refused = svc.bridge_probe_message("goldminer_bridge", "refused", url)
    timeout = svc.bridge_probe_message("goldminer_bridge", "timeout", url)
    protocol = svc.bridge_probe_message("goldminer_bridge", "protocol", url)
    assert "端口无监听" in refused and "桥进程未启动" in refused
    assert "端口已监听但无响应" in timeout and "终端显示“运行中”不代表查询可用" in timeout
    assert "应答格式不符" in protocol
    # 未知原因兜底仍给出 URL，不丢上下文
    assert url in svc.bridge_probe_message("goldminer_bridge", "unreachable", url)


def test_live_sell_scope_default_platform_only_and_invalid_fallback(monkeypatch):
    """卖出范围默认口径：缺键/非法 → platform_only（默认关闭，不代卖存量仓）。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    assert svc.live_sell_scope() == "platform_only"
    monkeypatch.setattr(
        svc, "load_config", lambda: _cfg(False, tq_trade={"live_sell_scope": ""}))
    assert svc.live_sell_scope() == "platform_only"
    monkeypatch.setattr(
        svc, "load_config", lambda: _cfg(False, tq_trade={"live_sell_scope": "xyz"}))
    assert svc.live_sell_scope() == "platform_only"
    monkeypatch.setattr(
        svc, "load_config",
        lambda: _cfg(False, tq_trade={"live_sell_scope": "full"}))
    assert svc.live_sell_scope() == "full"
    monkeypatch.setattr(
        svc, "load_config",
        lambda: _cfg(False, tq_trade={"live_sell_scope": "platform_only"}))
    assert svc.live_sell_scope() == "platform_only"


def test_resolve_session_sell_scope_prefers_session_then_global(monkeypatch):
    """会话级档位优先：NULL/非法回退全局；会话列有效时不读全局。"""
    monkeypatch.setattr(
        svc, "load_config", lambda: _cfg(False, tq_trade={"live_sell_scope": "full"}))
    # 会话显式 platform_only → 压过全局 full
    assert svc.resolve_session_sell_scope(
        SimpleNamespace(sell_scope="platform_only")) == "platform_only"
    # 会话显式 full → 保持
    assert svc.resolve_session_sell_scope(
        SimpleNamespace(sell_scope="full")) == "full"
    # NULL/空/非法 → 回退全局 full
    assert svc.resolve_session_sell_scope(
        SimpleNamespace(sell_scope=None)) == "full"
    assert svc.resolve_session_sell_scope(
        SimpleNamespace(sell_scope="xyz")) == "full"
    # 全局也非法 → 再回退默认 platform_only
    monkeypatch.setattr(
        svc, "load_config", lambda: _cfg(False, tq_trade={"live_sell_scope": "bad"}))
    assert svc.resolve_session_sell_scope(
        SimpleNamespace(sell_scope=None)) == "platform_only"


def test_default_engine_empty_or_invalid_falls_back_to_legacy(monkeypatch):
    """A-6：未配置/空/非法 → 沿用旧行为（ths_ui.enabled → ths，否则 iquant）。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    assert svc.default_engine() == "iquant"
    monkeypatch.setattr(
        svc, "load_config", lambda: _cfg(False, tq_trade={"default_engine": ""}))
    assert svc.default_engine() == "iquant"
    monkeypatch.setattr(
        svc, "load_config", lambda: _cfg(False, tq_trade={"default_engine": "xyz"}))
    assert svc.default_engine() == "iquant"
    monkeypatch.setattr(
        svc, "load_config", lambda: _cfg(True, tq_trade={"default_engine": ""}))
    monkeypatch.setattr(svc, "ThsUiDispatcher", object)
    assert svc.default_engine() == "ths"


def test_create_session_defaults_engine_to_iquant(db_session, monkeypatch):
    """无显式引擎且配置无 default_engine → 旧行为默认 iquant。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))
    data = svc.create_session(db_session, {"name": "s2"}, "live")
    assert data["engine"] == "iquant"


def test_create_session_uses_configured_default_engine(db_session, monkeypatch):
    """A-6：create_session 未显式选引擎时用 tq_trade.default_engine。"""
    monkeypatch.setattr(
        svc, "load_config", lambda: _cfg(False, tq_trade={"default_engine": "tdx"}))
    data = svc.create_session(db_session, {"name": "s3"}, "live")
    assert data["engine"] == "tdx"


def test_config_defaults_carry_new_tq_trade_keys():
    """A-7：core.config._defaults 与 SystemConfig TQ 通道段新键一一对应。"""
    from core.config import _defaults
    tq = _defaults()["tq_trade"]
    assert tq["default_engine"] == "tdx"
    assert tq["cancel_unfilled_minutes"] == 3


# ---------------- THS 文件交接层（file_builtin 双模式）配置 ----------------

def test_config_defaults_carry_new_ths_ui_keys():
    """文件交接层三键默认值：file_builtin / 5s / data/ths。"""
    from core.config import _defaults
    ths = _defaults()["ths_ui"]
    assert ths["execute_mode"] == "file_builtin"
    assert ths["execute_interval_seconds"] == 5
    assert ths["work_dir"] == "data/ths"


def test_ths_ui_config_exposes_file_mode_keys(monkeypatch):
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(True))
    data = svc.ths_ui_config()
    assert data["execute_mode"] == "file_builtin"
    assert data["execute_interval_seconds"] == 5
    assert data["work_dir"] == "data/ths"


def test_make_dispatcher_ths_passes_file_mode_params(monkeypatch):
    """ths 引擎构造 dispatcher 带出执行模式/间隔/工作目录/session_id。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(True))
    seen = {}

    class FakeThs:
        def __init__(self, **kw):
            seen.update(kw)

    monkeypatch.setattr(svc, "ThsUiDispatcher", FakeThs)
    svc.make_dispatcher("ths", "simulation", 7)
    assert seen["execute_mode"] == "file_builtin"
    assert seen["execute_interval_seconds"] == 5
    assert seen["work_dir"] == "data/ths"
    assert seen["session_id"] == 7


def test_position_cost_negative_cost_not_faked_with_price():
    """B5：柜台成本为负/0（送转分红后常见）→ 成本未知(0)，不得用 市值÷数量 冒充现价。

    旧实现回退成现价：盈亏恒 0、风控形同失效，且成本随价格每轮漂移。
    """
    from decimal import Decimal
    from core.engine.live_engine import _position_cost
    assert _position_cost({"cost": -475.0, "market_value": 727.0}, 20) == 0
    assert _position_cost({"cost": 0, "market_value": 727.0}, 20) == 0
    # 显式给出 cost=None（桥取不到成本属性）语义是「未知」，不是「没这一列」：
    # 绝不能回退 市值÷股数（=现价冒充成本，盈亏恒 0、成本每轮漂移）
    assert _position_cost({"cost": None, "market_value": 727.0}, 20) == 0
    # 字段缺失（旧桥/通道不给 cost）仍保留市值÷股数估值兜底
    assert _position_cost({"market_value": 727.0}, 20) == Decimal("36.35")
    assert _position_cost({"cost": 36.43}, 20) == Decimal("36.43")


def test_mirror_bridge_clears_stale_cost_when_unknown():
    """B5：镜面覆盖拿到未知成本时清回 0（未知标记），不留上一轮旧成本。"""
    from decimal import Decimal
    from datetime import date
    from core.engine.position import Position
    pos = Position("300413.SZ")
    pos.quantity = 20
    pos.avg_cost = Decimal("36.43")
    pos.mirror_bridge(20, 20, Decimal("0"), date(2026, 11, 10))
    assert pos.avg_cost == 0


# ---------------- 通道探活超时：与桥真实响应量级匹配 ----------------

def test_probe_timeout_matches_channel_response_scale():
    """窗口类 0.5s；HTTP 桥 5s（掘金 /ping 实测 3.02s，旧 1s 会误判离线）。"""
    assert svc._probe_timeout_for("tdx") == 0.5
    assert svc._probe_timeout_for("ths") == 0.5
    assert svc._probe_timeout_for("goldminer") == 5.0
    assert svc._probe_timeout_for("iquant") == 5.0


def test_probe_bridge_allows_three_second_ping(monkeypatch):
    """活线程慢应答在超时值以内 → 判在线（不因桥慢被误判离线）。"""
    class _SlowBridge:
        def heartbeat(self):
            time.sleep(0.4)   # 真桥 3s 量级，测试里等比缩小
            return True

    online, stale, note = svc._probe_online_quick("goldminer", _SlowBridge())
    assert online is True and stale is False and note == "ok"


def test_probe_budget_exhausted_reuses_last_known(monkeypatch):
    """探活预算耗尽：沿用上次已知值并给「沿用上次结果」标注，不发探针。"""
    svc._PROBE_CACHE["goldminer"] = (
        time.monotonic() - svc._PROBE_TTL_SECONDS - 1, True)

    class _Never:
        def heartbeat(self):
            raise AssertionError("预算耗尽时不应再发探针")

    online, stale, note = svc._probe_online_quick(
        "goldminer", _Never(), deadline=time.monotonic() - 0.01)
    assert online is True and stale is True
    assert note == "budget"
    assert "沿用上次结果" in svc._PROBE_NOTE_TEXT[note]


def test_probe_budget_exhausted_without_baseline_marks_offline(monkeypatch):
    """无历史结果且预算耗尽：暂判离线，但标注为「无上次结果」而非故障。"""
    class _Never:
        def heartbeat(self):
            raise AssertionError("预算耗尽时不应再发探针")

    online, stale, note = svc._probe_online_quick(
        "iquant", _Never(), deadline=time.monotonic() - 0.01)
    assert online is False and stale is True
    assert note == "budget_no_baseline"


def test_bridge_online_without_engine_reports_available(monkeypatch):
    """桥可见但会话未启动：显示可用 + 「无运行引擎」，不是通道故障。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False))

    class _Bridge:
        def __init__(self, **kw):
            pass

        def heartbeat(self):
            return True

    monkeypatch.setattr(svc, "HttpBridgeDispatcher", _Bridge)
    data = svc.channel_status(engine_key="goldminer", engines={})
    assert data["online"] is True
    assert data["engine_running"] is False
    assert "无运行引擎" in data["detail"]


# ---------------- TQ 账号文案：空配置 ≠ 功能不可用 ----------------

def test_tdx_detail_empty_account_is_client_state_not_missing_config(monkeypatch):
    """account 留空不是「未配置」：客户端不在时应说「未检测到登录账号」。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False, tq_trade={}))
    monkeypatch.setattr(svc, "_window_present", lambda key: False)
    data = svc.channel_status(engine_key="tdx", engines={})
    assert "未配置" not in data["detail"]
    assert "未检测到客户端登录账号" in data["detail"]


def test_tdx_detail_reads_connected_client_account(monkeypatch):
    """引擎已连上客户端 → 显示「客户端当前登录账号」（TQ 用客户端当前账号）。"""
    monkeypatch.setattr(svc, "load_config", lambda: _cfg(False, tq_trade={}))
    engine = SimpleNamespace(
        dispatcher=SimpleNamespace(_connected=True, _account_id=3))
    data = svc.channel_status(engine=engine, engine_key="tdx", engines={})
    assert data["detail"] == "客户端当前登录账号"


def test_tdx_detail_prefers_explicit_configured_account(monkeypatch):
    monkeypatch.setattr(
        svc, "load_config", lambda: _cfg(False, tq_trade={"account": "A1"}))
    data = svc.channel_status(engine_key="tdx", engines={})
    assert data["detail"] == "TQ 资金账号 A1"
