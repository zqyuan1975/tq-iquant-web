# -*- coding: utf-8 -*-
"""条件选股：方案、版本、运行、快照、成分、GP16 缓存（A1）。

设计边界（对应规划方案 §5）：
- 方案（scheme）可改名/停用，**版本（version）一经创建不可变**；
- 快照一旦冻结即只读，回测按 ``as_of`` 引用当时的成分；
- 每次快照记录三项指纹：``field_set_version``（字段集）、``spec_hash``（方案参数）、
  ``config_fingerprint``（权重/阈值组合），三者任一不同即判定「不可比」；
- GP16 缓存单独一表：它是导入物，不和选股结果混在一起，可独立重跑与清理。
"""

from sqlalchemy import (
    Boolean, Column, Date, DateTime, ForeignKey, Index, Integer, Numeric,
    String, Text, UniqueConstraint,
)
from sqlalchemy.sql import func

from .base import Base


class SelectionScheme(Base):
    """选股方案（逻辑分组：四风格五池 + 行业池，一池一方案）。"""

    __tablename__ = "selection_schemes"

    id = Column(Integer, primary_key=True)
    code = Column(String(40), nullable=False)          # 稳定标识，如 dividend
    name = Column(String(80), nullable=False)
    # growth / quality / value / dividend / industry:*
    pool_key = Column(String(40), nullable=False)
    description = Column(String(300), nullable=True)
    # active / archived
    status = Column(String(16), nullable=False, default="active")
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("code", name="uq_selection_schemes_code"),
        Index("ix_selection_schemes_pool", "pool_key", "status"),
    )


class SelectionSchemeVersion(Base):
    """方案版本：不可变快照。改权重/阈值 = 新建版本，历史版本永不改写。"""

    __tablename__ = "selection_scheme_versions"

    id = Column(Integer, primary_key=True)
    scheme_id = Column(Integer, ForeignKey("selection_schemes.id",
                                            ondelete="CASCADE"), nullable=False)
    version = Column(Integer, nullable=False)
    # 完整参数树（权重、阈值、准入、Top N、缩尾分位等）的 JSON
    spec_json = Column(Text, nullable=False)
    spec_hash = Column(String(16), nullable=False)
    # 参与打分的字段集版本与指纹
    field_set_version = Column(String(40), nullable=False)
    field_fingerprint = Column(String(16), nullable=False)
    # 权重和（未归一化时服务层拒绝保存）
    weight_sum = Column(Numeric(10, 6), nullable=True)
    # 经验权重未经回测校准：True 时前端强制显示「未校准」徽标
    uncalibrated = Column(Boolean, nullable=False, default=True)
    # draft / active / archived
    status = Column(String(16), nullable=False, default="draft")
    note = Column(String(300), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("scheme_id", "version",
                         name="uq_selection_scheme_versions_scheme_version"),
        Index("ix_selection_scheme_versions_hash", "spec_hash"),
    )


class SelectionRun(Base):
    """一次选股执行（即使失败也留痕，便于排查与幂等判断）。"""

    __tablename__ = "selection_runs"

    id = Column(Integer, primary_key=True)
    scheme_version_id = Column(
        Integer, ForeignKey("selection_scheme_versions.id",
                            ondelete="CASCADE"), nullable=False)
    # 决策日：所有 PIT 截断以此为上限
    as_of = Column(Date, nullable=False)
    # pending / running / succeeded / failed
    status = Column(String(16), nullable=False, default="pending")
    stock_count = Column(Integer, nullable=True)
    # 参数 + 数据窗口指纹，用于判定「同参数同数据」可复用
    params_hash = Column(String(16), nullable=True)
    error = Column(String(500), nullable=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_selection_runs_version_asof", "scheme_version_id", "as_of"),
        Index("ix_selection_runs_status", "status"),
    )


class SelectionSnapshot(Base):
    """冻结后的选股快照：回测与回看的唯一入口。"""

    __tablename__ = "selection_snapshots"

    id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("selection_runs.id",
                                        ondelete="CASCADE"), nullable=False)
    scheme_version_id = Column(Integer, nullable=False)
    as_of = Column(Date, nullable=False)
    # 三项指纹：任一不同 → 两次快照不可直接比较
    fingerprint = Column(String(16), nullable=False)
    field_set_version = Column(String(40), nullable=False)
    spec_hash = Column(String(16), nullable=False)
    config_fingerprint = Column(String(16), nullable=False)
    # 结果统计与数据质量披露（缺失率、被剔除原因计数、PIT 覆盖情况）
    stats_json = Column(Text, nullable=True)
    quality_json = Column(Text, nullable=True)
    frozen = Column(Boolean, nullable=False, default=False)
    # 发布到平台股票池后回填，便于回测按池引用
    published_pool_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("run_id", name="uq_selection_snapshots_run"),
        Index("ix_selection_snapshots_asof", "as_of"),
        Index("ix_selection_snapshots_pool", "published_pool_id"),
    )


class SelectionSnapshotStock(Base):
    """快照成分：入池与未入池都留行，``selected=False`` 附带排除原因。"""

    __tablename__ = "selection_snapshot_stocks"

    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, ForeignKey("selection_snapshots.id",
                                              ondelete="CASCADE"), nullable=False)
    code = Column(String(20), nullable=False)
    rank = Column(Integer, nullable=True)
    score = Column(Numeric(12, 6), nullable=True)
    selected = Column(Boolean, nullable=False, default=False)
    # 该行在哪个池里参与排序（四风格之一 / industry:xxx）
    pool_key = Column(String(40), nullable=False)
    # 硬门槛命中项：st / new_listing / pe_out_of_range / ...
    exclude_reason = Column(String(200), nullable=True)
    # 各因子原始值与分位（JSON），供明细页展示与复核
    factor_json = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("snapshot_id", "code",
                         name="uq_selection_snapshot_stocks_code"),
        Index("ix_selection_snapshot_stocks_rank", "snapshot_id", "rank"),
        Index("ix_selection_snapshot_stocks_selected", "snapshot_id", "selected"),
    )


class SelectionGpSnapshot(Base):
    """GP16 历史总市值缓存（D2 口径：按日期分片取，不重建股本）。

    唯一键 (code, as_of)：同一票同一日重复导入只会覆盖数值，不会刷行。
    缺值处理由服务层负责：停牌按 ``corporate_actions.end_date`` 顺延，
    无事件的空值保留 NULL 并在质量报告里计数，不做静默插值。
    """

    __tablename__ = "selection_gp_snapshots"

    id = Column(Integer, primary_key=True)
    code = Column(String(20), nullable=False)
    as_of = Column(Date, nullable=False)
    # GP16：当日总市值（万元）
    total_mv_wan = Column(Numeric(20, 4), nullable=True)
    # tq:GP16 / import:wide_db
    source = Column(String(40), nullable=False, default="tq:GP16")
    ingested_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("code", "as_of", name="uq_selection_gp_snapshots_code_asof"),
        Index("ix_selection_gp_snapshots_asof", "as_of"),
    )
