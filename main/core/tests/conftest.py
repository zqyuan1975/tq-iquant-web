import os
import tempfile
from pathlib import Path

# 测试环境禁用内嵌备份调度：集成测试用 `with TestClient(app)` 会触发 lifespan，
# 否则 15:10 之后会真的 VACUUM 真实 dev.db 并写备份记录/文件（测试不应有副作用）。
os.environ.setdefault("TQ_BACKUP_SCHEDULER_DISABLED", "1")
# P0-1（2026-11-23）：公司行为审计在回测启动/15:05 轮次后台自动触发，
# 会卡通道取数并写诊断表。测试进程统一关闭自动触发，避免异步写库与取数。
os.environ.setdefault("TQ_AUTO_CA_AUDIT_DISABLED", "1")
# N3（2026-11-23）：体检轮换抽样游标落临时文件，避免测试写仓库 data/。
import tempfile as _tempfile
os.environ.setdefault(
    "TQ_HEALTH_CURSOR_PATH",
    os.path.join(_tempfile.gettempdir(), "tq_health_sample_cursor_test.json"))
# 同理禁用增量回测调度（每日 15:10 会读真实 DB 并卡通达信取数）。
os.environ.setdefault("TQ_INCREMENTAL_SCHEDULER_DISABLED", "1")
# 告警外发（飞书 Webhook）：测试期间强制关闭，绝不给真实机器人发消息
# （本机 config.yaml 可能已配真实 webhook_url 且 enabled=true）。
os.environ.setdefault("TQ_NOTIFY_DISABLED", "1")
# 僵尸实盘会话清理：lifespan 用的是真实 SessionLocal（dependency_overrides 管不到），
# 集成用例 `with TestClient(app)` 会把正在运行的实盘会话写成 stopped
# （2026-11-19 09:45:27 实证：测试进程借 lifespan 改真实 dev.db）。
os.environ.setdefault("TQ_LIVE_STALE_RECOVERY_DISABLED", "1")
# 迁移闸（2026-11-20）：lifespan → core.db.init_db() → alembic upgrade head，
# 用的是真实配置解析出的库。`with TestClient(app)` 曾把生产 dev.db 从
# g4a5b6c7d8e9 静默迁到 h5b6c7d8e9f0（12:35 实证）。测试进程一律不迁移。
os.environ.setdefault("TQ_INIT_DB_DISABLED", "1")
# 双保险：库路径重定向到临时文件。即便闸被人关掉，lifespan 的初始化也不会落到
# main/data/dev.db；同时让「测试进程写真实库」这类事故从根上断掉。
_TEST_DB_PATH = Path(tempfile.gettempdir()) / ("tq_test_%d.db" % os.getpid())
os.environ.setdefault("TQ_DB_PATH", str(_TEST_DB_PATH))
# 旧交接目录清理按「库里的 running/stopping 会话」决定保留哪些目录；库切到临时
# 文件后读不到真实 running 会话，会误归档正在消费的会话目录 → 测试进程禁用。
os.environ.setdefault("TQ_HANDOFF_ARCHIVE_DISABLED", "1")
# TQ 通道闸（core/tq/gate.py）：测试进程默认关闭分级/时段策略，避免用例结果
# 依赖墙上时钟（例如 14:55–15:10 保护窗口会把默认 P1 调用整体拒掉）。
# 闸自身的用例走 reset_for_tests 注入配置，不受此开关影响。
os.environ.setdefault("TQ_GATE_FORCE_ALLOW", "1")

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from fastapi.testclient import TestClient

from core.models import Base
from core.main import app
from core.db import engine as _test_engine
from core.db import get_db

# 临时库没有走 alembic 迁移（init_db 已禁用），用模型元数据建表即可满足
# lifespan 的只读/恢复查询（僵尸回测、watchdog、归档调度等）。
Base.metadata.create_all(_test_engine)


def pytest_sessionfinish(session, exitstatus):
    """结束清理临时库（保留文件会在 %TEMP% 里累积）。"""
    try:
        _test_engine.dispose()
    except Exception:  # noqa: BLE001
        pass
    for suffix in ("", "-wal", "-shm"):
        try:
            Path(str(_TEST_DB_PATH) + suffix).unlink(missing_ok=True)
        except OSError:
            pass


@pytest.fixture
def db_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


@pytest.fixture
def test_client(db_session):
    def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    return TestClient(app)
