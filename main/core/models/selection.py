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
    Boolean, Column, Date, DateTime, ForeignKey, Index, Integer, LargeBinary,
    Numeric, String, Text, UniqueConstraint,
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


# ---------------------------------------------------------------------------
# 宽库导入仓（D1：宽库只在导入期读取；运行期只读这几张平台表）
# 单行元数据 + 每票一行（行情/估值/一致预期快照）+ 每票一行（财报序列）
# + 每票一行（分红事件序列）。序列统一 zlib(JSON) 压缩，控制主库体积。
# ---------------------------------------------------------------------------

class SelectionWarehouseMeta(Base):
    """导入仓元数据：固定单行（id=1），记录源路径、快照日与导入时间。"""

    __tablename__ = "selection_wh_meta"

    id = Column(Integer, primary_key=True)
    as_of = Column(Date, nullable=False)
    source_path = Column(String(400), nullable=False)
    source_size_mb = Column(Numeric(10, 1), nullable=True)
    imported_at = Column(DateTime, nullable=False, server_default=func.now())
    stock_count = Column(Integer, nullable=False, default=0)
    fin_count = Column(Integer, nullable=False, default=0)
    dividend_count = Column(Integer, nullable=False, default=0)
    # 覆盖与缺口披露（一致性检查、字段覆盖率、缺失项）
    counts_json = Column(Text, nullable=True)


class SelectionWarehouseStock(Base):
    """每票一行的导入快照（估值/上市信息/GO/GP/K 线派生指标）。"""

    __tablename__ = "selection_wh_stocks"

    code = Column(String(20), primary_key=True)
    name = Column(String(60), nullable=True)
    exchange = Column(String(10), nullable=True)
    sector_code = Column(String(40), nullable=True)
    sector_name = Column(String(60), nullable=True)
    pool = Column(String(40), nullable=True)
    tags = Column(String(200), nullable=True)
    is_financial = Column(Boolean, nullable=False, default=False)
    # 原始宽库 payload（已是 zlib 压缩的 JSON，导入时原样搬运）
    more_blob = Column(LargeBinary, nullable=True)
    info_blob = Column(LargeBinary, nullable=True)
    go_blob = Column(LargeBinary, nullable=True)
    gp_blob = Column(LargeBinary, nullable=True)
    # K 线派生：导入日收盘、近 252 日波动率（%）、近 252 日最大回撤（%）
    close = Column(Numeric(12, 4), nullable=True)
    volatility_pct = Column(Numeric(10, 4), nullable=True)
    max_drawdown_pct = Column(Numeric(10, 4), nullable=True)
    fin_periods = Column(Integer, nullable=False, default=0)
    fin_last_tag = Column(String(8), nullable=True)

    __table_args__ = (
        Index("ix_selection_wh_stocks_sector", "sector_name"),
    )


class SelectionWarehouseFin(Base):
    """每票一行的财报序列：tag_time / announce_time / 各 FN 值，整体压缩。"""

    __tablename__ = "selection_wh_fin"

    code = Column(String(20), primary_key=True)
    n_periods = Column(Integer, nullable=False, default=0)
    last_tag_time = Column(String(8), nullable=True)
    last_announce_time = Column(String(8), nullable=True)
    series_blob = Column(LargeBinary, nullable=False)


class SelectionWarehouseDividend(Base):
    """每票一行的分红事件序列：[[ex_date, year, bonus], ...]，整体压缩。"""

    __tablename__ = "selection_wh_dividend"

    code = Column(String(20), primary_key=True)
    n_events = Column(Integer, nullable=False, default=0)
    last_ex_date = Column(String(8), nullable=True)
    rows_blob = Column(LargeBinary, nullable=False)


class SelectionFactorValidation(Base):
    """因子稳定性验证摘要（单行）：导入时用宽库 K 线前向收益回算。

    只存汇总结果（均值 IC、ICIR、分组单调性），不存逐期明细；``as_of`` 与
    导入快照日绑定，重新导入即整行替换——避免验证结果与数据版本错位。
    """

    __tablename__ = "selection_factor_validations"

    id = Column(Integer, primary_key=True)
    as_of = Column(Date, nullable=False)
    horizon_days = Column(Integer, nullable=False, default=20)
    step_days = Column(Integer, nullable=False, default=21)
    n_periods = Column(Integer, nullable=False, default=0)
    # {factor: {mean_ic, icir, positive_ratio, quintile_means, monotonic, n_periods}}
    payload_json = Column(Text, nullable=False)
    computed_at = Column(DateTime, server_default=func.now())


class SelectionPoolRecall(Base):
    """池子有效性（召回）验证结果：池只是候选域，最终由公式择时决定买什么。

    口径：同一 as_of，分别取「公式在全市场入选集合」与「公式在池内入选集合」，
    召回率 = 池内入选 / 全市场入选；并给出池外漏票明细与成因分类。
    IC/分组单调性仅作参考数据，不参与任何决策。

    唯一键（snapshot_id, formula_id, horizon_days）：同一口径重复计算直接覆盖，
    避免结果与快照/公式版本错位。
    """

    __tablename__ = "selection_pool_recalls"

    id = Column(Integer, primary_key=True)
    as_of = Column(Date, nullable=False)
    formula_id = Column(Integer, nullable=False)
    snapshot_id = Column(Integer, nullable=False)
    horizon_days = Column(Integer, nullable=False, default=20)
    payload_json = Column(Text, nullable=False)
    computed_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("snapshot_id", "formula_id", "horizon_days",
                         name="uq_selection_pool_recall"),
    )

