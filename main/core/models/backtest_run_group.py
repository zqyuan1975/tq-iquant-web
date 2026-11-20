from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.sql import func

from .base import Base


class BacktestRunGroup(Base):
    """多组合回测运行组（N1，2026-11-18）。

    一次 ``run_multi`` 生成 1 个 group + N 条 ``backtest_records``（成员记录，
    各自完整、详情/分析可直接复用）。聚合曲线与共享现金口径的说明落在这里：

    * ``params_snapshot``：与成员记录同源的公共口径（区间/复权/执行档位等）；
    * ``aggregate_json``：``{dates, equity[], drawdown[], members[{portfolio_id,
      record_id}], initial_capital}``（净值按成员 total_value 求和，初始资金为
      各组合初始资金之和）。
    """

    __tablename__ = "backtest_run_groups"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    status = Column(String(10), nullable=False, default="running",
                    server_default="running")
    error_message = Column(Text, nullable=True)
    # 0021 E3：绑定 L2 拼盘头（可空；普通多组合回测为 NULL）。
    pairing_plan_id = Column(Integer, nullable=True)
    params_snapshot = Column(Text, nullable=True)
    aggregate_json = Column(Text, nullable=True)
    started_at = Column(DateTime, server_default=func.now())
    finished_at = Column(DateTime, nullable=True)
