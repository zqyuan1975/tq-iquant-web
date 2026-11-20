import logging
import os
import sys
import threading
from pathlib import Path
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from core.config import app_root, is_frozen_app, load_config, use_app_root

logger = logging.getLogger(__name__)

# 锚定到 main/ 目录（本文件位于 main/core/db.py），让 sqlite_path 无论
# 从哪个 cwd 启动都解析到同一绝对路径，消除对进程工作目录的依赖。
_MAIN_DIR = Path(__file__).resolve().parent.parent


def _db_path() -> Path:
    """数据库路径：优先 config.yaml，fallback 到应用根目录（可写）。

    ``TQ_DB_PATH`` 可显式覆盖（测试/演练用）：测试进程把它指到临时文件后，
    任何走 ``SessionLocal`` 的 lifespan 逻辑（含 ``init_db``）都碰不到生产 dev.db。
    相对路径锚定规则与 config.yaml 的 ``sqlite_path`` 完全一致。
    """
    override = os.environ.get("TQ_DB_PATH")
    if override:
        p = Path(override)
        if not p.is_absolute():
            p = (app_root() if use_app_root() else _MAIN_DIR) / p
        return p
    cfg = load_config()
    sqlite_path = cfg.get("database", {}).get("sqlite_path", "data/dev.db")
    p = Path(sqlite_path)
    if not p.is_absolute():
        # 打包版锚到 app_root()：安装目录不可写时自动落到 %LOCALAPPDATA%，
        # 避免 SQLite 在只读目录下建库失败。
        p = (app_root() if use_app_root() else _MAIN_DIR) / p
    return p


_db_file = _db_path()
SQLALCHEMY_DATABASE_URL = f"sqlite:///{_db_file.as_posix()}"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": 5},
)
SessionLocal = sessionmaker(bind=engine)

#: 进程内「长耗时库任务」互斥闸：自动备份（VACUUM INTO 长读事务）与增量回测
#: （跨计算的写事务）都由 15:10 的调度线程触发，同分钟撞车时会与实盘快照写入
#: 抢 SQLite 写锁（busy_timeout 5s 超时报 database is locked，2026-11-20 实测）。
#: 约定：两类任务开始前非阻塞抢闸，抢不到就让本轮调度跳过（60s 后重试），
#: 二者永不同时运行；闸只保护这两类维护任务，不介入实盘订单/撮合路径。
MAINTENANCE_GATE = threading.Lock()


@event.listens_for(engine, "connect")
def _enable_sqlite_fk(dbapi_conn, connection_record):
    """SQLite 逐连接 PRAGMA：外键约束 + WAL 日志 + 写锁等待（#9）。

    foreign_keys=ON 让 ondelete CASCADE/RESTRICT 生效（默认关闭）；
    journal_mode=WAL 读写不互斥，SSE 推送/轮询读与回测写并发时不再互相阻塞；
    busy_timeout=5000ms 写锁冲突时等待 5s 再报 database is locked。
    """
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.close()


def _alembic_revisions() -> str:
    """只读读取当前 alembic 版本（多 head 用逗号拼接），供启动迁移日志取证。"""
    from alembic.runtime.migration import MigrationContext

    with engine.connect() as conn:
        current = sorted(MigrationContext.configure(conn).get_current_heads())
    return ",".join(current) if current else "空"


def init_db():
    """应用数据库迁移（Alembic upgrade head），替代原 Base.metadata.create_all。

    审计 #22：schema 变更必须全部走迁移链而非旁路建表，避免 create_all 与迁移链漂移。
    已迁移到 head 的库是 no-op；迁移失败则抛异常阻止应用启动（fail fast）。

    **真实行为：应用每次启动都会自动迁移到 head**（`core.main` lifespan 调用本函数）。
    测试进程必须设置 ``TQ_INIT_DB_DISABLED=1`` 跳过——否则 ``with TestClient(app)``
    的 lifespan 会把真实 dev.db 静默升级（2026-11-20 12:35 实证：测试进程把生产库
    从 g4a5b6c7d8e9 迁到 h5b6c7d8e9f0）。

    注意：刻意不加载 alembic.ini —— 否则 env.py 会执行 fileConfig()，其默认
    disable_existing_loggers=True 会禁用 core.* 等业务 logger，污染运行期日志与测试 caplog。
    用无参 Config + 绝对路径显式指定 script_location/prepend_sys_path；
    url 由 alembic/env.py 的 _db_url() 从 config.yaml / TQ_DB_PATH 读取并锚定 main/
    （与 db.py 同源），因此无需 ini 的其余配置。
    """
    if os.environ.get("TQ_INIT_DB_DISABLED") == "1":
        return

    import sys

    from alembic import command
    from alembic.config import Config

    # 手动把 main/ 加入 sys.path：不依赖 alembic 的 prepend_sys_path（无 ini 时其
    # legacy 分割会把 Windows 盘符 D: 误切，见 alembic DeprecationWarning）。
    if str(_MAIN_DIR) not in sys.path:
        sys.path.insert(0, str(_MAIN_DIR))

    cfg = Config()
    script_dir = _MAIN_DIR / "alembic"
    if is_frozen_app():
        # 打包版：迁移脚本随安装目录分发（不是数据目录），优先取 exe 同级 alembic/
        packaged = Path(sys.argv[0]).resolve().parent / "alembic"
        if (packaged / "env.py").exists():
            script_dir = packaged
    cfg.set_main_option("script_location", str(script_dir))
    before = _alembic_revisions()
    command.upgrade(cfg, "head")
    after = _alembic_revisions()
    if after != before:
        # 自动迁移必须留痕：否则「库怎么变了」只能靠 alembic 的 console 日志
        # （pytest 下没有 FileHandler，真实迁移会完全无痕）。
        logger.warning("init_db：启动自动迁移 %s → %s（已对齐 head）", before, after)
    else:
        logger.info("init_db：数据库已在 head（%s）", after)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
