"""自愈父进程（core/supervisor.py）聚焦测试。

只测不启真实后端的纯逻辑：失败上限、无响应判定、父进程存活判断、子进程命令。
真实起停/杀进程的实测在交付说明里单独记录（避免测试套件动端口/卡通达信）。
"""
from __future__ import annotations

import os
import threading

from core import runtime, supervisor


class FakeChild:
    """最小子进程替身：poll 返回值可配置。"""

    def __init__(self, pid: int, poll_code=1):
        self.pid = pid
        self.poll_code = poll_code
        self.terminated = 0

    def poll(self):
        return self.poll_code

    def terminate(self):
        self.terminated += 1
        self.poll_code = 0

    def kill(self):
        self.poll_code = -9

    def wait(self, timeout=None):
        return self.poll_code


class AliveChild(FakeChild):
    def poll(self):
        return self.poll_code  # 默认 None 表示仍存活

    def __init__(self, pid: int):
        super().__init__(pid, poll_code=None)


def _no_instance(timeout=0.5):
    return None


def test_gives_up_after_max_failures(tmp_path, monkeypatch):
    """连续启动失败达上限 → 停止重试并返回 1（不无限重启）。"""
    monkeypatch.setattr(runtime, "app_root", lambda: tmp_path)
    spawned = []

    def spawn():
        child = FakeChild(pid=1000 + len(spawned))
        spawned.append(child)
        return child

    sup = supervisor.Supervisor(
        spawn_fn=spawn, find_fn=_no_instance, sleep_fn=lambda s: None,
        interval=0.01, startup_timeout=0, max_failures=3)
    assert sup.run() == 1
    assert len(spawned) == 3


def test_restarts_when_health_unresponsive(tmp_path, monkeypatch):
    """子进程存活但 /health 连续无响应 → 判定卡死并重启。"""
    monkeypatch.setattr(runtime, "app_root", lambda: tmp_path)
    spawned = []

    def spawn():
        child = AliveChild(pid=2000 + len(spawned))
        spawned.append(child)
        return child

    sup = supervisor.Supervisor(
        spawn_fn=spawn,
        ready_fn=lambda child: True,
        health_fn=lambda child: False,
        find_fn=_no_instance,
        sleep_fn=lambda s: None,
        interval=0.01, unhealthy_limit=2, max_failures=2, backoff_base=0.0)
    assert sup.run() == 1
    assert len(spawned) == 2
    assert all(child.terminated == 1 for child in spawned)


def test_stop_event_terminates_child(tmp_path, monkeypatch):
    """收到退出信号（stop_event）→ 结束子进程并返回 0。"""
    monkeypatch.setattr(runtime, "app_root", lambda: tmp_path)
    stop = threading.Event()
    spawned = []

    def spawn():
        stop.set()  # 拉起后立即要求退出
        child = AliveChild(pid=3000 + len(spawned))
        spawned.append(child)
        return child

    sup = supervisor.Supervisor(
        spawn_fn=spawn, ready_fn=lambda child: True,
        find_fn=_no_instance, sleep_fn=lambda s: None,
        interval=0.01, stop_event=stop)
    assert sup.run() == 0
    assert len(spawned) == 1
    assert spawned[0].terminated == 1


def test_existing_instance_is_not_duplicated(tmp_path, monkeypatch):
    """已有健康实例 → 监督进程直接退出，不再拉子进程（防双开）。"""
    monkeypatch.setattr(runtime, "app_root", lambda: tmp_path)
    spawned = []

    def spawn():
        spawned.append(1)
        return FakeChild(pid=4000)

    sup = supervisor.Supervisor(
        spawn_fn=spawn,
        find_fn=lambda timeout=0.5: {"pid": 123, "port": 8890, "version": "1.0"},
        sleep_fn=lambda s: None)
    assert sup.run() == 0
    assert spawned == []


def test_parent_process_alive():
    assert supervisor.parent_process_alive(os.getpid()) is True
    assert supervisor.parent_process_alive(0) is False


def test_child_command_source_mode():
    cmd = supervisor.child_command(8893)
    assert cmd[0]
    assert "--serve" in cmd
    assert cmd[-2:] == ["--port", "8893"]
    # 源码模式：复用 desktop.py（打包模式才复用 exe 本身）
    assert any("desktop.py" in item for item in cmd)
