"""数据备份记录（内嵌备份功能的可查询台账）。

每次自动/手动备份落一行：时间、文件、大小、类型、结果与说明。
- kind：auto（每日定时）/ manual（用户点「立即备份」）；
- status：success / skipped（磁盘空间不足等保护性跳过）/ failed；
- started_at / finished_at 为 Asia/Shanghai naive 时间（直接用于界面展示），
  created_at 沿用项目惯例的 UTC server_default。
"""
from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.sql import func

from .base import Base


class DataBackupRecord(Base):
    __tablename__ = "data_backup_records"

    id = Column(Integer, primary_key=True)
    kind = Column(String(10), nullable=False, index=True)
    status = Column(String(10), nullable=False, index=True)
    filename = Column(String(255), nullable=True)
    path = Column(String(500), nullable=True)
    size_bytes = Column(Integer, nullable=True)
    message = Column(Text, nullable=True)
    started_at = Column(DateTime, nullable=False, index=True)
    finished_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
