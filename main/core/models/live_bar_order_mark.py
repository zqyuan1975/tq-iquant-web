from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.sql import func

from .base import Base


class LiveBarOrderMark(Base):
    """「该 bar 已产生过下单动作」的持久幂等标记（2026-11-11）。

    `live_orders` 上的同 bar 同向门只在原单处于 submitted/partial/filled 时生效；
    原单一旦 canceled/rejected，重启重驱同一根 bar（1d/1w/1mon 典型）会再次下单。
    本表只记「这一根 bar 这个方向已经下过单」，不看订单当前终态，重启后仍生效。

    唯一索引保证并发/多线程下不会漏（应用层「先查后写」不能替代）。
    `seq` 区分同一次决策内的大额拆单子单（0=普通单/拆单父单，1..n=子单序号）；
    追价重下不写标记（同一张单的加价重报，不是新决策）。
    """

    __tablename__ = "live_bar_order_marks"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"), nullable=False)
    portfolio_strategy_id = Column(Integer, nullable=False)
    strategy_id = Column(Integer, nullable=False)
    stock_code = Column(String(20), nullable=False)
    bar_time = Column(DateTime, nullable=False)
    direction = Column(String(4), nullable=False)
    seq = Column(Integer, nullable=False, default=0, server_default="0")
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index(
            "ux_live_bar_order_marks_key",
            "live_session_id", "portfolio_strategy_id", "strategy_id",
            "stock_code", "bar_time", "direction", "seq",
            unique=True,
        ),
    )
