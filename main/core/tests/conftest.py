import os

# 测试环境禁用内嵌备份调度：集成测试用 `with TestClient(app)` 会触发 lifespan，
# 否则 15:10 之后会真的 VACUUM 真实 dev.db 并写备份记录/文件（测试不应有副作用）。
os.environ.setdefault("TQ_BACKUP_SCHEDULER_DISABLED", "1")
# 同理禁用增量回测调度（每日 15:10 会读真实 DB 并卡通达信取数）。
os.environ.setdefault("TQ_INCREMENTAL_SCHEDULER_DISABLED", "1")
# 告警外发（飞书 Webhook）：测试期间强制关闭，绝不给真实机器人发消息
# （本机 config.yaml 可能已配真实 webhook_url 且 enabled=true）。
os.environ.setdefault("TQ_NOTIFY_DISABLED", "1")

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from fastapi.testclient import TestClient

from core.models import Base
from core.main import app
from core.db import get_db


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
