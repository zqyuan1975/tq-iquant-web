"""L4 日志轮转配置解析测试（默认关闭，不认识新键时行为不变）。"""
from core import logging_config


def test_rotate_settings_defaults_to_off(monkeypatch):
    monkeypatch.setattr("core.config.load_config", lambda: {})
    assert logging_config._rotate_settings() == (False, 14)


def test_rotate_settings_daily_enabled(monkeypatch):
    monkeypatch.setattr("core.config.load_config", lambda: {
        "logging": {"daily_rotate": True, "backup_count": 7}})
    assert logging_config._rotate_settings() == (True, 7)


def test_rotate_settings_invalid_count_falls_back(monkeypatch):
    monkeypatch.setattr("core.config.load_config", lambda: {
        "logging": {"daily_rotate": True, "backup_count": "bad"}})
    assert logging_config._rotate_settings() == (True, 14)


def test_rotate_settings_config_error_falls_back(monkeypatch):
    def _boom():
        raise RuntimeError("bad config")

    monkeypatch.setattr("core.config.load_config", _boom)
    assert logging_config._rotate_settings() == (False, 14)
