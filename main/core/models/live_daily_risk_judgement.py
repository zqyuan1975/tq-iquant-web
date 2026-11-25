"""日线风控两段式裁定（15:05 预判 / 16:00 终审）的持久化。

每行 = 某交易日、某组合策略的某只持仓在某个阶段（phase）的风控判定快照：
- ``preliminary``：15:05 收盘落定轮次判定，只留档，**不产生可执行指令**；
- ``final``：16:00 终审（通达信本地日线已落盘），唯一可被次日集合竞价放行的依据。

唯一键 ``(live_session_id, strategy_id, stock_code, trade_date, phase)`` 保证
同一交易日同一持仓只有一条终审裁定（幂等重跑只更新，不产生新指令）。
"""
from datetime import datetime

from sqlalchemy import (
    Boolean, Column, Date, DateTime, Integer, Numeric, String, UniqueConstraint,
)

from .base import Base


class LiveDailyRiskJudgement(Base):
    __tablename__ = "live_daily_risk_judgements"
    __table_args__ = (
        UniqueConstraint(
            "live_session_id", "strategy_id", "stock_code", "trade_date", "phase",
            name="uq_live_daily_risk_judgement",
        ),
    )

    id = Column(Integer, primary_key=True)
    live_session_id = Column(Integer, nullable=False, index=True)
    portfolio_id = Column(Integer, nullable=False)
    strategy_id = Column(Integer, nullable=False)
    stock_code = Column(String(16), nullable=False)
    # 信号 bar 日期（日线 00:00 所属交易日），即待放行单的 signal_bar_date
    trade_date = Column(Date, nullable=False, index=True)
    # preliminary / final
    phase = Column(String(12), nullable=False)
    triggered = Column(Boolean, nullable=False, default=False)
    # stop_loss / take_profit / trailing_stop；未触发为 ""
    condition = Column(String(20), nullable=False, default="")
    close_price = Column(Numeric(18, 3), nullable=True)
    avg_cost = Column(Numeric(18, 4), nullable=True)
    highest_price = Column(Numeric(18, 4), nullable=True)
    threshold = Column(Numeric(10, 6), nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self) -> str:  # pragma: no cover - 调试用
        return (
            "<LiveDailyRiskJudgement %s %s %s %s %s triggered=%s>"
            % (self.live_session_id, self.strategy_id, self.stock_code,
               self.trade_date, self.phase, self.triggered)
        )
