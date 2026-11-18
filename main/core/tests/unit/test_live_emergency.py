"""M2 全局应急开关单元测试（服务层 + 引擎同步回调）。"""
from core.services import live_service


class _FakeEngine:
    def __init__(self):
        self.halt = None

    def set_global_halt(self, halted):
        self.halt = halted


def test_emergency_defaults_off(db_session):
    state = live_service.get_emergency(db_session)
    assert state["halt_new_positions"] is False


def test_pause_and_resume_syncs_running_engines(db_session):
    engine = _FakeEngine()
    engines = {1: engine}
    state = live_service.set_global_halt(
        db_session, True, "盘中异常", "tester", engines)
    assert state["halt_new_positions"] is True
    assert state["reason"] == "盘中异常"
    assert engine.halt is True

    state = live_service.set_global_halt(
        db_session, False, "恢复", "tester", engines)
    assert state["halt_new_positions"] is False
    assert engine.halt is False


def test_liquidate_requires_explicit_confirm(db_session):
    engine = _FakeEngine()
    ok, error = live_service.liquidate_all(
        db_session, "yes", "test", {1: engine})
    assert ok is False
    assert error[0] == 400


def test_liquidate_calls_engine(db_session):
    class _LiquidEngine:
        def __init__(self):
            self.reason = None

        def emergency_liquidate_all(self, reason=None):
            self.reason = reason
            return {"submitted": 2, "skipped": 0}

    engine = _LiquidEngine()
    ok, data = live_service.liquidate_all(
        db_session, "LIQUIDATE", "风控事件", {7: engine})
    assert ok is True
    assert engine.reason == "风控事件"
    assert data["results"][0]["session_id"] == 7
    assert data["results"][0]["submitted"] == 2
