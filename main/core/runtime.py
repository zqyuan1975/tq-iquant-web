"""运行期实例信息（``data/runtime/server.json``）与进程级单实例锁。

打包版监听端口是动态的（默认 ``8888``，被占用时顺延到 ``8898``），所以任何
「找到正在跑的后端」的逻辑都必须读本模块写的 ``server.json``（实际端口 / pid /
版本），**禁止硬编码端口**。自愈父进程（``core/supervisor.py``）、桌面壳第二次
双击、健康探活全部走这里。

单实例由两把文件锁保证（Windows ``msvcrt`` / POSIX ``fcntl``，进程退出自动释放）：

- ``data/runtime/supervisor.lock``：同一时刻只允许一个自愈父进程；
- ``data/runtime/server.lock``：同一时刻只允许一个真正的后端（避免两个后端
  同时连同一个 SQLite）。

``/health`` 由 ``health_payload()`` 生成（版本 / 运行时长 / 实际端口 / pid），
轻量无鉴权，供父进程与桌面壳探活。
"""
from __future__ import annotations

import json
import logging
import os
import socket
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from core.config import app_root
from core.version import APP_VERSION

logger = logging.getLogger(__name__)

#: 健康载荷身份标记：只认自己人，避免误连本机其它恰好返回 200 的服务
APP_MARKER = "tq-iquant"

DEFAULT_PORT = 8888
PORT_SPAN = 11  # 8888~8898

#: 进程启动时刻（/health 运行时长基准；mark_serving 不清零）
_STARTED_MONOTONIC = time.monotonic()

_BOUND_PORT: Optional[int] = None
_SERVING = False


# ---------------------------------------------------------------- 路径
def runtime_dir() -> Path:
    """``<app_root>/data/runtime``（自动创建）。"""
    path = app_root() / "data" / "runtime"
    path.mkdir(parents=True, exist_ok=True)
    return path


def server_json_path() -> Path:
    return runtime_dir() / "server.json"


def server_lock_path() -> Path:
    return runtime_dir() / "server.lock"


def supervisor_lock_path() -> Path:
    return runtime_dir() / "supervisor.lock"


# ---------------------------------------------------------------- server.json
def read_server_info() -> Optional[dict]:
    """读 ``server.json``；缺失/损坏返回 None（绝不抛）。"""
    path = server_json_path()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def write_server_info(port: int, *, version: Optional[str] = None) -> dict:
    """原子写实际监听端口 / pid / 版本（临时文件 + ``os.replace``）。"""
    path = server_json_path()
    info = {
        "app": APP_MARKER,
        "pid": os.getpid(),
        "port": int(port),
        "version": version or APP_VERSION,
        "started_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "app_root": str(app_root()),
        "supervised": os.environ.get("TQ_SUPERVISED") == "1",
    }
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(info, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return info


def clear_server_info(pid: Optional[int] = None) -> None:
    """删除 ``server.json``；给 ``pid`` 时只删属于该进程的那份（防误删新实例）。"""
    path = server_json_path()
    try:
        if pid is not None:
            info = read_server_info()
            if info and int(info.get("pid") or 0) != int(pid):
                return
        path.unlink()
    except FileNotFoundError:
        pass
    except (OSError, ValueError):
        logger.warning("清理 server.json 失败（忽略）", exc_info=True)


# ---------------------------------------------------------------- 健康探活
def mark_serving(port: Optional[int]) -> None:
    """uvicorn 启动时调用：记录本进程实际端口并落 ``server.json``。

    ``port`` 为 None（例如 ``uvicorn core.main:app`` 但拿不到端口）时不写文件，
    只让 ``/health`` 尽力报告（端口为 null），避免污染运行目录。
    """
    global _BOUND_PORT, _SERVING
    _SERVING = True
    _BOUND_PORT = int(port) if port else None
    if _BOUND_PORT:
        try:
            write_server_info(_BOUND_PORT)
        except OSError:
            logger.warning("写入 server.json 失败（忽略）", exc_info=True)


def mark_stopped() -> None:
    """进程正常退出时调用：清理属于本进程的 ``server.json``。"""
    global _SERVING
    _SERVING = False
    clear_server_info(os.getpid())


def health_payload() -> dict:
    """``GET /health`` 的响应体（版本 / 运行时长 / 实际端口 / pid）。"""
    info = read_server_info() or {}
    return {
        "ok": True,
        "app": APP_MARKER,
        "version": APP_VERSION,
        "pid": os.getpid(),
        "port": _BOUND_PORT if _BOUND_PORT is not None else info.get("port"),
        "started_at": info.get("started_at"),
        "uptime_seconds": int(time.monotonic() - _STARTED_MONOTONIC),
        "supervised": os.environ.get("TQ_SUPERVISED") == "1",
    }


def is_our_health(payload: Optional[dict]) -> bool:
    """是否本程序后端返回的健康载荷（身份标记 + ok）。"""
    return bool(payload) and payload.get("ok") is True and payload.get("app") == APP_MARKER


def http_health(port: int, timeout: float = 1.0) -> Optional[dict]:
    """``GET http://127.0.0.1:<port>/health``；连不上/非 JSON 返回 None。"""
    url = "http://127.0.0.1:%d/health" % int(port)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            if resp.status != 200:
                return None
            return json.loads(resp.read().decode("utf-8") or "{}")
    except (urllib.error.URLError, OSError, ValueError):
        return None


def find_running_instance(timeout: float = 0.25,
                          span: int = PORT_SPAN) -> Optional[dict]:
    """找到真正在服务的本程序后端：``server.json`` 优先，其次扫端口区间。

    返回 ``{"port", "pid", "version", "health", ...}``；没有则 None。
    只认 ``/health`` 的 ``app=tq-iquant`` 标记，绝不把别的本地服务当成自己。
    """
    info = read_server_info()
    candidates = []
    if info and info.get("port"):
        candidates.append(info)
    try:
        preferred = int(os.environ.get("TQ_PORT") or 0)
    except ValueError:
        preferred = 0
    if preferred:
        candidates.append({"port": preferred})
    candidates.extend({"port": p} for p in range(DEFAULT_PORT, DEFAULT_PORT + span))
    seen = set()
    for item in candidates:
        try:
            port = int(item.get("port"))
        except (TypeError, ValueError):
            continue
        if port in seen:
            continue
        seen.add(port)
        payload = http_health(port, timeout=timeout)
        if not is_our_health(payload):
            continue
        result = dict(item)
        result["port"] = port
        result["pid"] = payload.get("pid", item.get("pid"))
        result["version"] = payload.get("version", item.get("version"))
        result["health"] = payload
        return result
    return None


# ---------------------------------------------------------------- 端口
def _port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(("127.0.0.1", int(port)))
        except OSError:
            return False
    return True


def pick_port(preferred: Optional[int] = None, span: int = PORT_SPAN) -> int:
    """选一个可监听端口：``preferred``/``TQ_PORT`` 起，被占用则顺延 ``span`` 个。"""
    if preferred is None:
        try:
            preferred = int(os.environ.get("TQ_PORT") or DEFAULT_PORT)
        except ValueError:
            preferred = DEFAULT_PORT
    for port in range(preferred, preferred + span):
        if _port_free(port):
            if port != preferred:
                logger.warning("端口 %d 被占用，改用 %d", preferred, port)
            return port
    raise RuntimeError("端口 %d~%d 均被占用，请先关闭其它程序" % (preferred, preferred + span - 1))


def resolve_bound_port(argv: Optional[list] = None) -> Optional[int]:
    """解析本进程实际监听端口：``TQ_BOUND_PORT`` 优先，其次命令行 ``--port``。"""
    raw = os.environ.get("TQ_BOUND_PORT")
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    args = list(sys.argv if argv is None else argv)
    for idx, item in enumerate(args):
        if item == "--port" and idx + 1 < len(args):
            try:
                return int(args[idx + 1])
            except ValueError:
                return None
        if item.startswith("--port="):
            try:
                return int(item.split("=", 1)[1])
            except ValueError:
                return None
    return None


def auto_shutdown_suppressed() -> bool:
    """``--serve`` 子进程 / 自愈模式不做「关标签页即退出」。"""
    return (os.environ.get("TQ_NO_AUTO_SHUTDOWN") == "1"
            or os.environ.get("TQ_SUPERVISED") == "1")


# ---------------------------------------------------------------- 单实例锁
class InstanceLock:
    """跨进程独占文件锁；持有进程退出（含强杀）时由操作系统自动释放。

    用法：``lock = InstanceLock(path); if not lock.acquire(): ...``。
    持有者 pid 写在同目录 ``<lock>.pid`` 旁挂文件（仅供诊断；Windows 字节锁
    会阻止其它句柄读取被锁区间，所以 pid 不写进锁文件本身）。
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.pid_path = self.path.with_name(self.path.name + ".pid")
        self._fh = None

    @property
    def held(self) -> bool:
        return self._fh is not None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self.path, "a+b")
        try:
            fh.seek(0)
            if fh.read(1) == b"":
                fh.seek(0)
                fh.write(b"0")
                fh.flush()
            fh.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            fh.close()
            return False
        self._fh = fh
        try:
            self.pid_path.write_text(str(os.getpid()), encoding="ascii")
        except OSError:
            pass
        return True

    def release(self) -> None:
        fh = self._fh
        self._fh = None
        if fh is None:
            return
        try:
            fh.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        finally:
            fh.close()
        # 只删自己写的 pid 旁挂文件（probe_lock 拿到锁后立刻释放，不能删真正持有者的）
        try:
            if self.pid_path.read_text(encoding="ascii", errors="ignore").strip() == str(os.getpid()):
                self.pid_path.unlink()
        except OSError:
            pass

    def __enter__(self):
        if not self.acquire():
            raise RuntimeError("无法获取实例锁：%s" % self.path)
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
        return False


def read_lock_owner_pid(path: Path) -> Optional[int]:
    """尽力读锁的持有者 pid（旁挂 ``<lock>.pid``，仅供日志/诊断）。"""
    pid_path = Path(path).with_name(Path(path).name + ".pid")
    try:
        return int(pid_path.read_text(encoding="ascii", errors="ignore").strip() or 0) or None
    except (OSError, ValueError):
        return None


def probe_lock(path: Path) -> Tuple[bool, Optional[int]]:
    """探测锁是否已被别的进程持有，返回 ``(是否空闲, 持有者 pid)``。"""
    lock = InstanceLock(path)
    if lock.acquire():
        lock.release()
        return True, None
    return False, read_lock_owner_pid(path)
