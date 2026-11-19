"""桌面壳启动器（打包版入口）。

行为：
1. 解析可写根目录（安装目录不可写时回落 %LOCALAPPDATA%\\创懿基金量化投资平台），
   首次启动从 config.template.yaml 生成 config.yaml，并切换工作目录到该根目录
   （config.yaml 里的相对路径 data/dev.db、data/ths 都以此为准）。
2. 单实例：先按 ``data/runtime/server.json`` + ``/health`` 找已运行的后端（端口
   动态，8888~8898，不硬编码）；没有就持 ``data/runtime/server.lock`` 自起后端。
   已有实例在跑时本进程只开界面，关窗口不杀后端（后端可能是自愈子进程）。
3. 后台线程启动 uvicorn，等待 ``/health`` 就绪。
4. 优先用 pywebview + WebView2 开原生窗口；WebView2 缺失/CLR 加载失败时
   **自动回退默认浏览器**（验收底线：不允许因此启动失败）。
5. 窗口关闭 → 通知 uvicorn 退出；浏览器模式下由前端心跳/关闭探针负责退出
   （core.services.auto_shutdown），进程本身驻留等待。

命令行（打包版与源码版一致）：
- 默认（双击）：单进程，后端 + 界面；
- ``--supervise``：自愈父进程（``core/supervisor.py``），常驻探活并重启子进程；
- ``--serve``：仅后端子进程（由父进程拉起，自动加 ``TQ_SUPERVISED=1``）；
- ``--no-supervise``：显式保持默认单进程（调试用，行为与不带参数一致）。
"""
import logging
import os
import sys
import threading
import time
import urllib.error
import urllib.request
import webbrowser

APP_TITLE = "创懿基金量化投资平台"

logger = logging.getLogger("desktop")


def _setup_basic_logging() -> None:
    """uvicorn 起来前先把启动信息落到 stderr（打包版无控制台时仅内存）。"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _healthy(url: str, timeout: float = 1.0) -> bool:
    try:
        with urllib.request.urlopen(url + "/health", timeout=timeout) as resp:
            return resp.status == 200
    except (urllib.error.URLError, OSError):
        return False


def _start_server(port: int):
    """起 uvicorn 线程；``TQ_BOUND_PORT`` 供 lifespan 写 server.json / /health。"""
    os.environ["TQ_BOUND_PORT"] = str(port)
    import uvicorn

    from core.main import app

    config = uvicorn.Config(
        app, host="127.0.0.1", port=port, log_config=None, access_log=False
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, name="uvicorn", daemon=True)
    thread.start()
    return server, thread


def _wait_ready(url: str, timeout: float = 60.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _healthy(url):
            return True
        time.sleep(0.25)
    return False


def _initial_window_size() -> tuple:
    """按屏幕工作区推算初始窗口尺寸，避免在小尺寸笔记本上超出可视区。

    返回 (宽, 高)。工作区读取失败时回退 1440×900（与原行为一致）。
    """
    try:
        import ctypes
        from ctypes import wintypes

        rect = wintypes.RECT()
        # SPI_GETWORKAREA：不含任务栏的桌面工作区（像素）
        ok = ctypes.windll.user32.SystemParametersInfoW(
            0x0030, 0, ctypes.byref(rect), 0
        )
        if ok:
            screen_w = rect.right - rect.left
            screen_h = rect.bottom - rect.top
        else:
            screen_w = ctypes.windll.user32.GetSystemMetrics(0)
            screen_h = ctypes.windll.user32.GetSystemMetrics(1)
    except Exception:  # noqa: BLE001 — 非 Windows/API 异常直接回退默认尺寸
        return 1440, 900
    if screen_w <= 0 or screen_h <= 0:
        return 1440, 900
    width = max(900, min(1440, int(screen_w * 0.96)))
    height = max(600, min(900, int(screen_h * 0.94)))
    return width, height


def _open_window(url: str) -> bool:
    """pywebview 原生窗口；任何失败都返回 False 交由浏览器回退。"""
    try:
        import webview
    except Exception:  # noqa: BLE001 — 依赖缺失/CLR 加载失败都走回退
        logger.exception("pywebview 导入失败，回退默认浏览器")
        return False
    try:
        width, height = _initial_window_size()
        # min_size 略小于初始尺寸：布局在 ≤1100px 宽已切抽屉+表格内滚动，
        # 窗口最小 900×600 不会小于布局最小可用宽度
        webview.create_window(
            APP_TITLE, url, width=width, height=height, min_size=(900, 600)
        )
        webview.start()
        return True
    except Exception:  # noqa: BLE001
        logger.exception("WebView2 窗口启动失败，回退默认浏览器")
        return False


def _run_ui(url: str, server) -> int:
    """打开界面并等待退出；``server is None`` 表示复用已有后端，不拥有其生命周期。"""
    force_browser = "--browser" in sys.argv or os.environ.get("TQ_BROWSER") == "1"
    no_open = "--no-open" in sys.argv  # 仅自检/冒烟用：只起后端，不开界面

    if no_open:
        if server is None:
            logger.info("（--no-open）复用的是已运行实例，本进程直接退出：%s", url)
            return 0
        logger.info("已跳过界面打开（--no-open），后端运行中：%s", url)
        while not server.should_exit:
            time.sleep(0.5)
        return 0

    opened = False if force_browser else _open_window(url)
    if not opened:
        webbrowser.open(url)
        logger.info("已用默认浏览器打开 %s（关闭窗口后进程自动退出）", url)
        if server is None:
            # 复用已有实例（可能是 --supervise 的子进程）：关窗口不杀它
            return 0
        while not server.should_exit:
            time.sleep(0.5)
        return 0

    # 原生窗口已关闭：只停自己起的后端，复用的实例交给其监督进程
    if server is not None:
        server.should_exit = True
        time.sleep(0.5)
    return 0


def _wait_for_instance(timeout: float):
    """另一个实例正在启动时，等它写出 server.json 并通过 /health。"""
    from core import runtime

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        found = runtime.find_running_instance(timeout=0.3)
        if found:
            return found
        time.sleep(1.0)
    return None


def main() -> int:
    # 自愈模式与后端子进程模式交给 supervisor（同一条源码/打包路径）
    if "--supervise" in sys.argv or "--serve" in sys.argv:
        from core.supervisor import main as supervisor_main

        return supervisor_main(sys.argv[1:])

    _setup_basic_logging()

    os.environ.setdefault("PYTHONUTF8", "1")
    from core import runtime
    from core.config import app_root, ensure_config

    root = app_root()
    (root / "data").mkdir(parents=True, exist_ok=True)
    (root / "logs").mkdir(parents=True, exist_ok=True)
    cfg_path = ensure_config()
    os.chdir(root)  # config.yaml 内的相对路径（data/dev.db、data/ths）以此为准
    logger.info("数据目录：%s", root)
    logger.info("配置文件：%s", cfg_path)

    running = runtime.find_running_instance()
    if running:
        port = int(running["port"])
        url = f"http://127.0.0.1:{port}"
        logger.info(
            "检测到已在运行的后端：%s（pid=%s，版本=%s），直接复用",
            url, running.get("pid"), running.get("version"))
        return _run_ui(url, None)

    # 单实例：拿不到 server.lock 说明另一个后端正在启动/正在服务
    lock = runtime.InstanceLock(runtime.server_lock_path())
    server = None
    if not lock.acquire():
        logger.warning("另一个后端实例正在启动（server.lock 被占用），等待其就绪…")
        found = _wait_for_instance(60.0)
        if not found:
            logger.error("等待另一个实例就绪超时（60s），请查看 logs/core.log")
            return 3
        url = f"http://127.0.0.1:{int(found['port'])}"
        logger.info("已连接到就绪的实例：%s（pid=%s）", url, found.get("pid"))
        return _run_ui(url, None)

    try:
        port = runtime.pick_port()
        url = f"http://127.0.0.1:{port}"
        server, _thread = _start_server(port)
        if not _wait_ready(url):
            logger.error("后端 60s 内未就绪，请查看 logs/core.log")
            server.should_exit = True
            return 2
        logger.info("后端就绪：%s", url)
        return _run_ui(url, server)
    finally:
        if server is not None:
            server.should_exit = True
        runtime.clear_server_info(os.getpid())
        lock.release()


if __name__ == "__main__":
    sys.exit(main())
