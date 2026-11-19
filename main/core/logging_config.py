import logging
import logging.handlers
import os
import sys
from pathlib import Path


def _under_pytest() -> bool:
    """是否在 pytest 测试进程内。测试期间不挂 FileHandler——否则单测日志会写进 core.log
    污染真实日志；且 core 进程占用 core.log 时，测试进程的 RotatingFileHandler 轮转改名
    会报 WinError 32（文件被占用）。caplog 不依赖 FileHandler，去掉不影响日志断言。"""
    return os.environ.get("PYTEST_CURRENT_TEST") is not None or "pytest" in sys.modules


def _rotate_settings() -> tuple:
    """日志轮转配置：``logging.daily_rotate``（默认 false）+ ``backup_count``（默认 14）。

    默认关闭 → 走原 ``RotatingFileHandler``（10MB×5），与改动前逐字一致；
    开启 → 按日切分（``TimedRotatingFileHandler``，midnight）。配置读取失败一律回退关闭。
    """
    try:
        from core.config import load_config

        raw = (load_config() or {}).get("logging")
        raw = raw if isinstance(raw, dict) else {}
        daily = bool(raw.get("daily_rotate", False))
        try:
            count = int(raw.get("backup_count", 14))
        except (TypeError, ValueError):
            count = 14
        return daily, max(count, 1)
    except Exception:  # noqa: BLE001 — 配置异常不得阻断日志初始化
        return False, 14


def _log_dir() -> Path:
    """日志目录：打包版/便携模式固定写可写根目录 ``logs/``，开发态写仓库 ``logs/``。"""
    from core.config import app_root, use_app_root

    if use_app_root():
        return app_root() / "logs"
    return Path(__file__).resolve().parent.parent.parent / "logs"


def _file_handler(log_file: Path) -> logging.Handler:
    """按 ``logging.daily_rotate`` 建文件 handler（core.log / access.log 共用同一规则）。"""
    daily, backup_count = _rotate_settings()
    if daily:
        return logging.handlers.TimedRotatingFileHandler(
            log_file, when="midnight", backupCount=backup_count, encoding="utf-8")
    return logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")


def _attach_uvicorn_access_log(formatter: logging.Formatter) -> None:
    """把 uvicorn 的访问日志接到 ``logs/access.log``（文件 + 原有控制台）。

    2026-11-19 排查实证：8001 进程的 access log 只进控制台，stop/start 的 HTTP
    调用无从取证（谁在什么时刻调了 stop 查不到）。uvicorn 默认给 ``uvicorn.access``
    单独挂 console handler 且 ``propagate=False``，所以 root 的 core.log 收不到。
    这里额外挂一个文件 handler，轮转规则与 core.log 完全一致（同一
    ``logging.daily_rotate`` / ``backup_count``），各自独立文件、互不冲突。
    """
    access = logging.getLogger("uvicorn.access")
    access.setLevel(logging.INFO)
    if any(getattr(h, "_core_access_logging", False) for h in access.handlers):
        return  # 幂等：uvicorn reload / lifespan 重复进入
    handler = _file_handler(_log_dir() / "access.log")
    handler.setFormatter(formatter)
    handler._core_access_logging = True
    access.addHandler(handler)
    # 不向 root 传播：避免 access 行重复写进 core.log（体积/轮转干扰业务日志）
    access.propagate = False


def setup_logging():
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # 幂等：uvicorn reload / lifespan 重复进入时不重复挂 handler。
    if not any(getattr(h, "_core_logging", False) for h in root.handlers):
        if not _under_pytest():
            # Nuitka 打包后 __file__ 位于 <exe目录>/core/，.parent×3 会跑到分发目录的上一级；
            # 打包版（及 TQ_APP_ROOT 便携模式）日志固定写到可写根目录的 logs/。
            log_dir = _log_dir()
            log_dir.mkdir(parents=True, exist_ok=True)
            file_handler = _file_handler(log_dir / "core.log")
            file_handler.setFormatter(formatter)
            file_handler._core_logging = True
            root.addHandler(file_handler)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler._core_logging = True
        root.addHandler(console_handler)

    # 访问日志落文件（测试进程不挂文件 handler，避免污染真实日志/占用冲突）
    if not _under_pytest():
        try:
            _attach_uvicorn_access_log(formatter)
        except Exception:  # noqa: BLE001 — 访问日志失败不得阻断启动
            logging.getLogger(__name__).warning(
                "挂载 uvicorn access.log 失败（忽略）", exc_info=True)

    # 压低第三方 HTTP 客户端噪音：httpx/httpcore 默认每拍一行 "HTTP Request ... 200/404"，
    # 会快速轮转冲掉盘中关键日志（14:30 日终、13:47 熔断实时日志因此丢失）。只留 WARNING+。
    for noisy in ("httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
