"""运行期实例信息 / 单实例锁（core/runtime.py）聚焦测试。

覆盖自愈与单实例依赖的关键事实：
- server.json 原子读写、只按 pid 精确清理；
- /health 载荷带身份标记与端口；
- 端口发现优先 server.json，且只认本程序（不误连其它本地 200 服务）；
- 跨进程文件锁互斥（两个后端不可能同时进）。
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from core import runtime


@pytest.fixture
def runtime_root(tmp_path, monkeypatch):
    """把 app_root 指向临时目录，避免污染真实 data/runtime。"""
    monkeypatch.setenv("TQ_APP_ROOT", str(tmp_path))
    monkeypatch.setattr(runtime, "app_root", lambda: tmp_path)
    monkeypatch.delenv("TQ_PORT", raising=False)
    return tmp_path


def test_server_json_roundtrip_and_pid_scoped_clear(runtime_root):
    info = runtime.write_server_info(8890)
    assert info["app"] == runtime.APP_MARKER
    assert info["pid"] == os.getpid()
    assert info["port"] == 8890
    assert runtime.server_json_path().exists()

    loaded = runtime.read_server_info()
    assert loaded is not None and loaded["port"] == 8890

    # 别的 pid 不清（防误删新实例写的文件）
    runtime.clear_server_info(pid=os.getpid() + 1)
    assert runtime.server_json_path().exists()

    runtime.clear_server_info(pid=os.getpid())
    assert not runtime.server_json_path().exists()
    assert runtime.read_server_info() is None


def test_clear_server_info_never_raises_when_missing(runtime_root):
    runtime.clear_server_info()
    runtime.clear_server_info(pid=12345)


def test_health_payload_has_identity_and_port(runtime_root):
    runtime.mark_serving(8891)
    try:
        payload = runtime.health_payload()
        assert payload["ok"] is True
        assert payload["app"] == runtime.APP_MARKER
        assert payload["port"] == 8891
        assert payload["pid"] == os.getpid()
        assert payload["uptime_seconds"] >= 0
        assert runtime.is_our_health(payload) is True
    finally:
        runtime.mark_stopped()
        runtime.mark_serving(None)


def test_is_our_health_rejects_foreign_service():
    assert runtime.is_our_health(None) is False
    assert runtime.is_our_health({"ok": True}) is False
    assert runtime.is_our_health({"ok": True, "app": "other"}) is False


def test_find_running_instance_prefers_server_json(runtime_root, monkeypatch):
    runtime.write_server_info(8892)
    monkeypatch.setattr(
        runtime, "http_health",
        lambda port, timeout=1.0: {"ok": True, "app": runtime.APP_MARKER,
                                   "pid": 4321, "port": port, "version": "9.9.9"})
    found = runtime.find_running_instance()
    assert found is not None
    assert found["port"] == 8892
    assert found["pid"] == 4321
    assert found["version"] == "9.9.9"


def test_find_running_instance_ignores_foreign_health(runtime_root, monkeypatch):
    monkeypatch.setattr(runtime, "http_health", lambda port, timeout=1.0: {"ok": True})
    assert runtime.find_running_instance() is None


def test_pick_port_skips_occupied(runtime_root, monkeypatch):
    monkeypatch.setattr(runtime, "_port_free", lambda port: port == 8889)
    assert runtime.pick_port() == 8889


def test_pick_port_raises_when_all_occupied(runtime_root, monkeypatch):
    monkeypatch.setattr(runtime, "_port_free", lambda port: False)
    with pytest.raises(RuntimeError):
        runtime.pick_port()


def _lock_holder_script() -> str:
    return (
        "import sys\n"
        "from core.runtime import InstanceLock\n"
        "sys.exit(0 if InstanceLock(sys.argv[1]).acquire() else 7)\n"
    )


def test_instance_lock_is_exclusive_across_processes(runtime_root):
    """父进程持锁时，另一个进程必须拿不到（两个后端不能同时连同一个库）。"""
    import core

    main_dir = Path(core.__file__).resolve().parents[1]
    lock_path = runtime_root / "server.lock"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(main_dir)
    cmd = [sys.executable, "-c", _lock_holder_script(), str(lock_path)]

    lock = runtime.InstanceLock(lock_path)
    assert lock.acquire() is True
    try:
        blocked = subprocess.run(
            cmd, cwd=str(main_dir), env=env, capture_output=True, timeout=60)
        assert blocked.returncode == 7, blocked.stderr.decode("utf-8", "ignore")
    finally:
        lock.release()

    # 释放后另一个进程能拿到，证明锁随进程/句柄释放
    acquired = subprocess.run(
        cmd, cwd=str(main_dir), env=env, capture_output=True, timeout=60)
    assert acquired.returncode == 0, acquired.stderr.decode("utf-8", "ignore")


def test_probe_lock_reports_owner_pid(runtime_root):
    lock_path = runtime_root / "supervisor.lock"
    lock = runtime.InstanceLock(lock_path)
    assert lock.acquire()
    try:
        free, owner = runtime.probe_lock(lock_path)
        assert free is False
        assert owner == os.getpid()
    finally:
        lock.release()
    assert runtime.probe_lock(lock_path)[0] is True
