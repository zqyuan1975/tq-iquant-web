from sqlalchemy import Boolean, Column, Integer, String, DateTime, Numeric
from sqlalchemy.sql import func

from .base import Base


class LiveSession(Base):
    __tablename__ = "live_sessions"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    mode = Column(String(10), nullable=False)
    # 交易引擎：iquant(QMT 通道) / tdx(通达信TQ) / ths(同花顺UI)。
    # 为 NULL 表示迁移前的历史会话，运行时按全局默认引擎（ths_ui.enabled 判定）解析，
    # 保持升级前行为不变；新建会话一律显式写入。
    # 历史 qmt 会话（通道已移除）由 session_engine 白名单回退到默认引擎。
    engine = Column(String(10), nullable=True)
    # 实盘资金基准（账户总资产口径，元）：None=各组合用自身 initial_capital；
    # 设置=会话内所有组合以该值起账（资金占比/单仓/敞口等比率配置等比缩放），
    # 多组合各自独立记账。启动后继承持仓的市值会从起账现金中扣除（避免与基准双计）。
    live_capital = Column(Numeric(18, 2), nullable=True)
    # 执行模式：alert（半自动，只预警不下单）/ auto（现有自动交易）。
    # 迁移前历史会话写 auto；新建会话由 live_service 显式写 alert。
    execution_mode = Column(String(10), nullable=False, server_default="auto")
    # 外部信号注入（0011）：通达信条件预警输出文件路径。NULL=不启用（走 TQ 公式）；
    # 配置后信号来源替换为文件，TQ 公式链路停用（两条路径互斥）。
    signal_file_path = Column(String(500), nullable=True)
    # 14:50 提前收盘价（仅 1d 日线层，会话级开关，B 口径）：默认 False 表示
    # 1d/1w/1mon 都在**周期收盘价落定后**（15:05 轮次）用完整 bar 算信号、次日开盘
    # 执行。True 时仅 1d 提前：用快照合成 14:50 1m bar 收盘价定信号，委托价取
    # 14:52 起始 1m bar 开盘价（14:51 兜底，两者都无则不下单），**该轮信号当日
    # 提交、尾盘成交**（不受 risk_timing=next_day 顺延）；1w/1mon 不受影响
    # （仍走 15:05 收盘落定 + 次日执行）。
    advance_close_enabled = Column(
        Boolean, default=False, server_default="0", nullable=False)
    # 执行时点 HH:MM（默认 14:51；限定 14:51–14:59，须在 14:50 分钟 bar 闭合后）。
    # 实际轮次不早于 14:52（委托价需要 14:52 起始 1m bar 的开盘价）。
    advance_close_time = Column(
        String(5), nullable=False, server_default="14:51", default="14:51")
    # 幂等标记：该交易日 14:52 提前轮次已执行（YYYY-MM-DD，跨重启不重复跑）
    advance_close_done_date = Column(String(10), nullable=True)
    # 幂等标记：该交易日 15:05「收盘落定」日线级别轮次（1d/1w/1mon）已执行
    # （YYYY-MM-DD，跨重启不重复跑；B 开启时只用于 1w/1mon 周期末轮次）
    daily_bars_done_date = Column(String(10), nullable=True)
    status = Column(String(10), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    started_at = Column(DateTime, nullable=True)
    stopped_at = Column(DateTime, nullable=True)
