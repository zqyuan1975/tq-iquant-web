"""随包分发的自愈父进程（无需任何外部脚本 / 计划任务脚本）。

职责边界
--------
- **本模块（父进程）**：拉起真正的服务子进程，按固定间隔探活 ``/health``，
  子进程退出或连续无响应时退避重启；连续失败达上限后停止重试并写明确日志。
  进程级崩溃的兜底只在这里，进程内的 ``EngineWatchdog`` 不负责重启进程。
- **子进程**（``--serve``）：只跑 FastAPI/uvicorn 后端，把实际端口 / pid / 版本
  写入 ``data/runtime/server.json``；端口是动态的（8888~8898），父进程与桌面壳
  都读该文件，**不硬编码端口**。
- **孤儿防护**：父进程把 pid 传给子进程（``TQ_SUPERVISOR_PID``），子进程内的
  看门狗线程发现父进程消失后自行优雅退出，不会留下孤儿长期占用端口。

单实例
------
``data/runtime/supervisor.lock`` 保证只跑一个监督进程；``server.lock`` 保证只有
一个真正的后端（避免两个后端同时连同一个 SQLite）。两把锁都是操作系统级文件锁，
进程被强杀时自动释放。

日志
----
- ``logs/supervisor.log``：监督进程自己的生命周期 / 探活 / 重启 / 放弃重试；
- ``logs/supervisor-child.log``：子进程的 stdout/stderr（启动崩溃原因在这里）；
- 子进程内部日志仍写 ``logs/core.log``（``core.logging_config.setup_logging``）。

入口
----
- 打包版：``tq-iquant.exe --supervise``（自愈）/ ``tq-iquant.exe --serve``（子进程）；
- 源码：``python -m core.supervisor``（默认即自愈）/ ``python desktop.py --supervise``。
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Callable, Optional

from core import runtime
from core.config import is_frozen_app

logger = logging.getLogger("supervisor")

DEFAULT_INTERVAL_SECONDS = 10.0
DEFAULT_HEALTH_TIMEOUT_SECONDS = 3.0
DEFAULT_UNHEALTHY_LIMIT = 3
DEFAULT_MAX_FAILURES = 5
DEFAULT_BACKOFF_BASE_SECONDS = 3.0
DEFAULT_BACKOFF_MAX_SECONDS = 60.0
DEFAULT_STARTUP_TIMEOUT_SECONDS = 90.0
DEFAULT_STABLE_SECONDS = 60.0
PARENT_CHECK_INTERVAL_SECONDS = 5.0

_LOG_CONFIGURED = False

_CREATE_NO_WINDOW = 0x08000000  # Windows: 子进程不弹控制台


# ---------------------------------------------------------------- 运行目录 / 日志
def _prepare_app_root() -> Path:
    """与桌面壳同口径：解析可写根目录、建 data/logs、生成配置、切 cwd。"""
    from core.config import app_root, ensure_config

    root = app_root()
    (root / "data").mkdir(parents=True, exist_ok=True)
    (root / "logs").mkdir(parents=True, exist_ok=True)
    ensure_config()
    os.chdir(root)
    return root


def _setup_supervisor_logging(root: Path) -> None:
    """监督进程日志：``logs/supervisor.log`` + stderr（幂等）。"""
    global _LOG_CONFIGURED
    if _LOG_CONFIGURED:
        return
    _LOG_CONFIGURED = True
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        file_handler = RotatingFileHandler(
            root / "logs" / "supervisor.log",
            maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError:
        pass
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


# ---------------------------------------------------------------- 子进程命令
def child_command(port: Optional[int] = None) -> list:
    """拉起 ``--serve`` 子进程的命令行（打包版复用 exe，源码版复用 desktop.py）。"""
    if is_frozen_app():
        cmd = [sys.executable, "--serve"]
    else:
        entry = Path(__file__).resolve().parent.parent / "desktop.py"
        cmd = [sys.executable, str(entry), "--serve"]
    if port:
        cmd += ["--port", str(int(port))]
    return cmd


# ---------------------------------------------------------------- 父进程存活判断
def parent_process_alive(pid: int) -> bool:
    """父进程是否还活着（Windows 用 OpenProcess+WaitForSingleObject，POSIX 用 kill(0)）。"""
    if pid <= 0:
        return False
    if os.name != "nt":
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
    import ctypes

    SYNCHRONIZE = 0x00100000
    WAIT_TIMEOUT = 0x00000102
    kernel32 = ctypes.windll.kernel32
    handle = kernel32.OpenProcess(SYNCHRONIZE, False, int(pid))
    if not handle:
        return False
    try:
        return kernel32.WaitForSingleObject(handle, 0) == WAIT_TIMEOUT
    finally:
        kernel32.CloseHandle(handle)


def _start_parent_watchdog(server, interval: float = PARENT_CHECK_INTERVAL_SECONDS) -> None:
    """子进程内看门狗：父进程消失 → 请求 uvicorn 优雅退出（不留孤儿端口）。"""
    raw = os.environ.get("TQ_SUPERVISOR_PID")
    if not raw:
        return
    try:
        parent_pid = int(raw)
    except ValueError:
        return

    def _watch() -> None:
        while not getattr(server, "should_exit", False):
            time.sleep(max(float(interval), 0.5))
            if parent_process_alive(parent_pid):
                continue
            logger.warning(
                "监督进程 pid=%d 已退出，服务子进程 pid=%d 自行退出（不留孤儿端口）",
                parent_pid, os.getpid())
            server.should_exit = True
            return

    threading.Thread(target=_watch, name="supervisor-parent-watchdog", daemon=True).start()


# ---------------------------------------------------------------- 监督进程
class Supervisor:
    """拉起 / 探活 / 重启服务子进程的循环（可注入依赖，便于单测）。"""

    def __init__(
        self,
        *,
        spawn_fn: Optional[Callable[[], object]] = None,
        ready_fn: Optional[Callable[[object], bool]] = None,
        health_fn: Optional[Callable[[object], bool]] = None,
        find_fn: Optional[Callable[..., Optional[dict]]] = None,
        sleep_fn: Callable[[float], None] = time.sleep,
        mono_fn: Callable[[], float] = time.monotonic,
        interval: float = DEFAULT_INTERVAL_SECONDS,
        health_timeout: float = DEFAULT_HEALTH_TIMEOUT_SECONDS,
        unhealthy_limit: int = DEFAULT_UNHEALTHY_LIMIT,
        max_failures: int = DEFAULT_MAX_FAILURES,
        backoff_base: float = DEFAULT_BACKOFF_BASE_SECONDS,
        backoff_max: float = DEFAULT_BACKOFF_MAX_SECONDS,
        startup_timeout: float = DEFAULT_STARTUP_TIMEOUT_SECONDS,
        stable_seconds: float = DEFAULT_STABLE_SECONDS,
        port: Optional[int] = None,
        stop_event: Optional[threading.Event] = None,
    ):
        self._spawn_fn = spawn_fn or self._spawn_child
        self._ready_fn = ready_fn or self._wait_ready_default
        self._health_fn = health_fn or self._health_default
        self._find_fn = find_fn or runtime.find_running_instance
        self._sleep_fn = sleep_fn
        self._mono = mono_fn
        self._interval = max(float(interval), 0.1)
        self._health_timeout = float(health_timeout)
        self._unhealthy_limit = max(int(unhealthy_limit), 1)
        self._max_failures = max(int(max_failures), 1)
        self._backoff_base = max(float(backoff_base), 0.0)
        self._backoff_max = max(float(backoff_max), 0.0)
        self._startup_timeout = max(float(startup_timeout), 0.0)
        self._stable_seconds = max(float(stable_seconds), 0.0)
        self._port = int(port) if port else None
        self._stop = stop_event or threading.Event()
        self._child_log = None
        # 真实子进程的 pid（只有真正 Popen 出来的才有；注入 spawn_fn 的替身为 None，
        # 用来保证 _terminate 绝不 taskkill 掉无关 pid）
        self._child_pid: Optional[int] = None
        # 真正后端的 pid（从 server.json 读到并经 /health 确认；Windows venv 的
        # python.exe 是 redirector，Popen pid ≠ 后端 pid）
        self._backend_pid: Optional[int] = None
        # 本次拉起的墙钟时刻（校验 server.json 的 started_at 不是上次崩溃残留）
        self._spawn_ts: Optional[float] = None

    # ------------------------------------------------------------ 循环
    def run(self) -> int:
        failures = 0
        backoff = self._backoff_base
        while not self._stop.is_set():
            # 已有健康实例（例如用户手动打开的另一份程序）→ 不重复拉起，避免双开
            existing = self._find_fn(timeout=0.25)
            if existing and existing.get("pid"):
                logger.info(
                    "发现已有后端实例在运行（pid=%s port=%s version=%s），监督进程退出（不重复启动）",
                    existing.get("pid"), existing.get("port"), existing.get("version"))
                return 0

            try:
                child = self._spawn_fn()
            except Exception:  # noqa: BLE001 — 拉起失败按一次失败计数，不崩溃
                logger.exception("拉起服务子进程失败（第 %d 次连续失败）", failures + 1)
                failures += 1
                if failures >= self._max_failures:
                    logger.error(
                        "连续 %d 次无法拉起服务子进程，已停止自动重试；"
                        "请检查 logs/supervisor-child.log / logs/core.log 后手动启动", failures)
                    return 1
                self._sleep(backoff)
                backoff = min(max(backoff * 2, self._backoff_base), self._backoff_max)
                continue

            logger.info(
                "已拉起服务子进程 pid=%s，等待 /health 就绪（最长 %.0f 秒）",
                getattr(child, "pid", "?"), self._startup_timeout)
            ready = self._ready_fn(child)
            if self._stop.is_set():
                logger.info("收到退出信号（启动阶段），停止服务子进程 pid=%s", getattr(child, "pid", "?"))
                self._clear_child_info(child)
                self._terminate(child)
                return 0
            if not ready:
                code = child.poll()
                logger.error(
                    "服务子进程未就绪（pid=%s，退出码=%s，启动超时 %.0f 秒）；准备第 %d 次重启",
                    getattr(child, "pid", "?"), code, self._startup_timeout, failures + 1)
                self._clear_child_info(child)
                self._terminate(child)
                failures += 1
                if failures >= self._max_failures:
                    logger.error(
                        "连续 %d 次启动失败，已停止自动重试；"
                        "请检查 logs/supervisor-child.log / logs/core.log 后手动启动", failures)
                    return 1
                self._sleep(backoff)
                backoff = min(max(backoff * 2, self._backoff_base), self._backoff_max)
                continue

            logger.info(
                "服务子进程就绪 pid=%s port=%s，开始每 %.0f 秒探活 /health",
                getattr(child, "pid", "?"), self._port, self._interval)
            healthy_since = self._mono()
            reason = self._monitor(child)
            code = child.poll()
            self._clear_child_info(child)
            self._terminate(child)
            if reason == "stop":
                logger.info("收到退出信号，已停止服务子进程 pid=%s", getattr(child, "pid", "?"))
                return 0

            ran = max(self._mono() - healthy_since, 0.0)
            if reason == "unresponsive":
                failures += 1
            elif ran >= self._stable_seconds:
                failures = 1
            else:
                failures += 1
            logger.warning(
                "服务子进程 pid=%s 退出（原因=%s，退出码=%s，本次健康运行 %.0f 秒），"
                "连续失败 %d/%d，%.0f 秒后重启",
                getattr(child, "pid", "?"), reason, code, ran,
                failures, self._max_failures, backoff)
            if failures >= self._max_failures:
                logger.error(
                    "连续 %d 次运行失败，已停止自动重试；"
                    "请检查 logs/supervisor.log / logs/supervisor-child.log / logs/core.log "
                    "后手动启动", failures)
                return 1
            self._sleep(backoff)
            backoff = min(max(backoff * 2, self._backoff_base), self._backoff_max)
        logger.info("监督循环已停止")
        return 0

    # ------------------------------------------------------------ 拉起 / 探活
    def _spawn_child(self):
        from core.config import app_root

        cmd = child_command(self._port)
        env = os.environ.copy()
        env["TQ_SUPERVISED"] = "1"
        env["TQ_NO_AUTO_SHUTDOWN"] = "1"
        env["TQ_SUPERVISOR_PID"] = str(os.getpid())
        child_log = app_root() / "logs" / "supervisor-child.log"
        child_log.parent.mkdir(parents=True, exist_ok=True)
        self._child_log = open(child_log, "ab", buffering=0)
        kwargs = dict(
            cwd=str(app_root()),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=self._child_log,
            stderr=subprocess.STDOUT,
        )
        if os.name == "nt":
            kwargs["creationflags"] = _CREATE_NO_WINDOW
        logger.info("启动命令：%s", " ".join(cmd))
        try:
            child = subprocess.Popen(cmd, **kwargs)
        except Exception:
            try:
                self._child_log.close()
            except OSError:
                pass
            self._child_log = None
            raise
        self._child_pid = int(child.pid)
        self._backend_pid = None
        self._spawn_ts = time.time()
        return child

    def _clear_child_info(self, child) -> None:
        """清掉死掉的子进程写的 ``server.json``（只清属于本子进程的那份）。"""
        info = runtime.read_server_info() or {}
        info_pid = int(info.get("pid") or 0)
        owned = {p for p in (getattr(child, "pid", None), self._backend_pid) if p}
        if info_pid and info_pid in owned:
            runtime.clear_server_info()

    def _is_fresh(self, info: dict) -> bool:
        """``server.json`` 是否本次拉起之后写的（挡掉上次崩溃残留的文件）。"""
        if self._spawn_ts is None:
            return True
        raw = info.get("started_at")
        if not raw:
            return True
        try:
            started = datetime.fromisoformat(str(raw))
        except ValueError:
            return True
        return started.timestamp() >= self._spawn_ts - 5.0

    def _wait_ready_default(self, child) -> bool:
        deadline = self._mono() + self._startup_timeout
        while self._mono() < deadline:
            if self._stop.is_set():
                return False
            if child.poll() is not None:
                return False
            info = runtime.read_server_info()
            port = (info or {}).get("port")
            # 不比对 Popen pid：Windows venv 的 python.exe 只是 redirector，
            # 真正跑后端的解释器是另一个 pid（server.json / /health 里的那个）。
            if (info and info.get("app") == runtime.APP_MARKER and port
                    and int(info.get("pid") or 0) != os.getpid()
                    and self._is_fresh(info)):
                port = int(port)
                payload = runtime.http_health(port, timeout=self._health_timeout)
                if (runtime.is_our_health(payload)
                        and int(payload.get("pid") or 0) == int(info.get("pid") or 0)):
                    self._port = port
                    self._backend_pid = int(info.get("pid"))
                    return True
            self._sleep(0.25)
        return False

    @staticmethod
    def _business_ok(payload) -> bool:
        """L6：``health.business_probe_enabled=true`` 时业务心跳也纳入探活判据。

        开关关闭（默认）→ 完全不看 ``business`` 字段，判活行为与旧版逐字一致；
        开关开启但载荷缺少该字段（旧子进程/降级）→ 不误判，仍按 HTTP 探活结果。
        """
        try:
            from core.config import load_config

            raw = (load_config() or {}).get("health")
            raw = raw if isinstance(raw, dict) else {}
            if not bool(raw.get("business_probe_enabled", False)):
                return True
        except Exception:  # noqa: BLE001 — 配置读取失败不得误杀子进程
            return True
        if not isinstance(payload, dict):
            return True
        business = payload.get("business")
        if not isinstance(business, dict):
            return True
        return business.get("ok") is not False

    def _health_default(self, child) -> bool:
        """按 ``server.json`` 的实际端口探活（端口动态，绝不硬编码）。"""
        if self._port:
            payload = runtime.http_health(self._port, timeout=self._health_timeout)
            if runtime.is_our_health(payload):
                return self._business_ok(payload)
        info = runtime.read_server_info() or {}
        port = info.get("port")
        if info.get("app") != runtime.APP_MARKER or not port:
            return False
        self._port = int(port)
        payload = runtime.http_health(self._port, timeout=self._health_timeout)
        return runtime.is_our_health(payload) and self._business_ok(payload)

    def _monitor(self, child) -> str:
        """返回 ``exited`` / ``unresponsive`` / ``stop``。"""
        misses = 0
        while True:
            if self._stop.wait(self._interval):
                return "stop"
            code = child.poll()
            if code is not None:
                logger.warning("服务子进程已退出（pid=%s，退出码=%s）", getattr(child, "pid", "?"), code)
                return "exited"
            if self._health_fn(child):
                misses = 0
                continue
            misses += 1
            logger.warning(
                "服务子进程 /health 探活失败（%d/%d，pid=%s port=%s）",
                misses, self._unhealthy_limit, getattr(child, "pid", "?"), self._port)
            if misses >= self._unhealthy_limit:
                logger.error(
                    "服务子进程连续 %d 次 /health 无响应（约 %.0f 秒），判定卡死并重启",
                    misses, misses * self._interval)
                return "unresponsive"

    @staticmethod
    def _taskkill_tree(pid: int) -> None:
        try:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(int(pid))],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, creationflags=_CREATE_NO_WINDOW, check=False)
        except OSError:
            pass

    def _terminate(self, child) -> None:
        if child is None:
            return
        pid = getattr(child, "pid", None)
        # Windows：venv 的 python.exe 是 redirector，Popen pid ≠ 真正后端 pid；
        # 必须用 /T 结束整棵进程树，否则会留下孤儿后端占着端口。
        # 只对本进程真正 Popen 出来的 pid 动手（注入替身/无关 pid 绝不 kill）。
        if os.name == "nt" and pid and self._child_pid == pid:
            self._taskkill_tree(pid)
            if self._backend_pid and self._backend_pid != pid \
                    and parent_process_alive(self._backend_pid):
                self._taskkill_tree(self._backend_pid)
        try:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    logger.warning("服务子进程 pid=%s 未在 10 秒内退出，强制结束", pid)
                    child.kill()
                    child.wait(timeout=5)
            else:
                try:
                    child.wait(timeout=5)
                except Exception:  # noqa: BLE001 — 已退出，等回收失败不影响重启
                    pass
        except Exception:  # noqa: BLE001 — 收尾失败不阻断重启
            logger.warning("结束服务子进程失败（pid=%s）", pid, exc_info=True)
        self._backend_pid = None
        if self._child_log is not None:
            try:
                self._child_log.close()
            except OSError:
                pass
            self._child_log = None

    def _sleep(self, seconds: float) -> None:
        self._sleep_fn(max(float(seconds), 0.0))


# ---------------------------------------------------------------- 子进程（--serve）
def serve(port: Optional[int] = None) -> int:
    """只跑后端的子进程：写 server.json、持 server.lock、父进程消失即退出。"""
    from core.logging_config import setup_logging

    _prepare_app_root()
    setup_logging()

    lock = runtime.InstanceLock(runtime.server_lock_path())
    if not lock.acquire():
        logger.error(
            "已有后端实例持有 data/runtime/server.lock（pid=%s），本进程退出，"
            "避免两个后端同时连同一个数据库",
            runtime.read_lock_owner_pid(runtime.server_lock_path()))
        return 3
    try:
        chosen = int(port) if port else runtime.pick_port()
        os.environ["TQ_BOUND_PORT"] = str(chosen)
        os.environ["TQ_NO_AUTO_SHUTDOWN"] = "1"
        import uvicorn

        from core.main import app

        config = uvicorn.Config(
            app, host="127.0.0.1", port=chosen, log_config=None, access_log=False)
        server = uvicorn.Server(config)
        _start_parent_watchdog(server)
        logger.info(
            "服务子进程启动：pid=%d port=%d supervised=%s",
            os.getpid(), chosen, os.environ.get("TQ_SUPERVISED") == "1")
        try:
            server.run()
        finally:
            runtime.clear_server_info(os.getpid())
        return 0
    finally:
        lock.release()


# ---------------------------------------------------------------- 监督入口
def _install_signal_handlers(stop: threading.Event) -> None:
    def _handle(signum, _frame) -> None:
        logger.info("收到信号 %s，准备退出（先停服务子进程）", signum)
        stop.set()

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            signal.signal(sig, _handle)
        except (ValueError, OSError):
            pass


def supervise(
    port: Optional[int] = None,
    interval: float = DEFAULT_INTERVAL_SECONDS,
    max_failures: int = DEFAULT_MAX_FAILURES,
) -> int:
    """自愈监督模式：单实例 + 已有实例检测 + 子进程循环。"""
    root = _prepare_app_root()
    _setup_supervisor_logging(root)
    logger.info(
        "自愈监督进程启动：pid=%d root=%s interval=%.0fs max_failures=%d",
        os.getpid(), root, interval, max_failures)

    supervisor_lock = runtime.InstanceLock(runtime.supervisor_lock_path())
    if not supervisor_lock.acquire():
        logger.warning(
            "另一个监督进程已在运行（supervisor.lock 持有者 pid=%s），本进程退出（不双开）",
            runtime.read_lock_owner_pid(runtime.supervisor_lock_path()))
        return 0
    try:
        existing = runtime.find_running_instance(timeout=0.3)
        if existing and existing.get("pid"):
            logger.info(
                "已有后端实例在运行（pid=%s port=%s version=%s），监督进程退出（不重复启动）",
                existing.get("pid"), existing.get("port"), existing.get("version"))
            return 0
        free, owner = runtime.probe_lock(runtime.server_lock_path())
        if not free:
            logger.error(
                "另一个后端进程持有 server.lock（pid=%s，可能正在启动或已卡死）；"
                "本监督进程不启动新实例。若确认该进程已卡死，请结束后再启动自愈模式", owner)
            return 3

        stop = threading.Event()
        _install_signal_handlers(stop)
        runner = Supervisor(
            port=port, interval=interval, max_failures=max_failures, stop_event=stop)
        return runner.run()
    finally:
        supervisor_lock.release()
        logger.info("监督进程退出：pid=%d", os.getpid())


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="创建自愈监督进程 / 服务子进程")
    parser.add_argument("--serve", action="store_true", help="后端子进程模式（由监督进程拉起）")
    parser.add_argument("--supervise", action="store_true", help="自愈监督模式（默认动作）")
    parser.add_argument("--port", type=int, default=None, help="指定端口（默认 8888~8898 取空闲）")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SECONDS,
                        help="探活间隔（秒）")
    parser.add_argument("--max-failures", type=int, default=DEFAULT_MAX_FAILURES,
                        help="连续失败上限（达到后停止自动重试）")
    args, _unknown = parser.parse_known_args(sys.argv[1:] if argv is None else argv)
    if args.serve:
        return serve(args.port)
    return supervise(args.port, args.interval, args.max_failures)


if __name__ == "__main__":
    sys.exit(main())
