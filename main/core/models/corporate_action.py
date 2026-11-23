"""公司行为事件（手工台账，2026-11-18）。

一张表承载 分红 / 送转 / 配股 / 退市 / 停牌 五类事件，回测与实盘读取同一张表、
共用 ``core/engine/corporate_actions.apply_day`` 判定内核。

口径要点（详见 docs/plans/0010-功能缺口第一批技术方案.md §2）：
- 不做自动抓取，全部手工录入；表为空 / 组合开关关闭时执行路径与改动前逐字一致。
- ``cash_per_share``（每股税后现金）只用于实盘与后复权记账腿；
  回测（前复权/后复权价空间）只认比例口径 ``cash_pct``。
- ``suspend`` 用 ``end_date`` 表示复牌日（含当日可交易），NULL = 至今未复牌。
"""
from sqlalchemy import (
    Column, Integer, String, Date, DateTime, Numeric, ForeignKey, UniqueConstraint, Index,
)
from sqlalchemy.sql import func

from .base import Base


class CorporateAction(Base):
    __tablename__ = "corporate_actions"

    id = Column(Integer, primary_key=True)
    code = Column(String(20), nullable=False)
    # dividend / split / rights / delist / suspend
    event_type = Column(String(16), nullable=False)
    # 除权除息日；停牌 = 停牌首日；退市 = 最后交易日
    ex_date = Column(Date, nullable=False)
    # 仅 suspend：复牌日（含当日可交易）；NULL = 至今未复牌
    end_date = Column(Date, nullable=True)
    # 每股现金分红（元，税后）：实盘 / 后复权记账腿口径
    cash_per_share = Column(Numeric(12, 6), nullable=True)
    # 分红占除权日前收比例（0.01 = 1%）：回测复权价空间口径
    cash_pct = Column(Numeric(10, 8), nullable=True)
    # 每股送转股数（0.3 = 10 送 3）；rights 时为配股比例
    share_ratio = Column(Numeric(12, 6), nullable=True)
    # 配股价（元），仅 rights
    rights_price = Column(Numeric(12, 6), nullable=True)
    note = Column(String(200), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("code", "event_type", "ex_date",
                         name="uq_corporate_actions_code_type_date"),
        Index("ix_corporate_actions_code_date", "code", "ex_date"),
    )


#: 诊断类型：价格序列出现显著断点但公司行为事件表里找不到对应登记事件。
DIAG_UNVERIFIED_EVENT = "corporate_action_unverified"


class CorporateActionDiagnostic(Base):
    """公司行为漏记诊断（只观测，不参与任何账务）。

    由 ``core/services/corporate_action_audit.py`` 写入：用「不复权收盘价 vs
    前复权收盘价」的比值跳变定位除权断点，再拿 ``corporate_actions`` 反查是否
    已登记。**只写本表**——绝不补录事件、绝不改已有事件、绝不改任何账本数值。
    唯一键 = (诊断类型, 股票, 断点日)，重复检测幂等更新，不刷行。
    """
    __tablename__ = "corporate_action_diagnostics"

    id = Column(Integer, primary_key=True)
    # 预留多类型（当前只有 corporate_action_unverified）
    diagnostic_type = Column(String(32), nullable=False, default=DIAG_UNVERIFIED_EVENT)
    code = Column(String(20), nullable=False)
    # 断点日 = 比值发生跳变的首个交易日
    break_date = Column(Date, nullable=False)
    # 断点前一个交易日
    prev_trade_date = Column(Date, nullable=True)
    # 复权比跳变倍数 k = ratio_after / ratio_before（k>1 = 价格下行断点）
    implied_ratio = Column(Numeric(12, 6), nullable=True)
    # 隐含除权跌幅（%），= (1 - 1/k) * 100
    implied_drop_pct = Column(Numeric(10, 4), nullable=True)
    # 断点前一日不复权收盘价 / 隐含除权参考价
    prev_close = Column(Numeric(12, 6), nullable=True)
    implied_ex_price = Column(Numeric(12, 6), nullable=True)
    # dividend / split / rights（启发式判定，仅作排查线索）
    suspected_event_type = Column(String(16), nullable=True)
    # 本次检测实际覆盖的数据窗口
    window_start = Column(Date, nullable=True)
    window_end = Column(Date, nullable=True)
    # 数据来源标识，如 tq:close_raw_vs_front
    source = Column(String(40), nullable=True)
    # 附加线索（JSON 文本：阈值、隐含每股派现/送转比例等）
    detail = Column(String(500), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("diagnostic_type", "code", "break_date",
                         name="uq_corporate_action_diag_key"),
        Index("ix_corporate_action_diag_code_date", "code", "break_date"),
    )


class LiveCorporateAction(Base):
    """实盘公司行为应用台账（分红入账重放依据）。

    为什么单独一张表：虚拟现金在 ``recover`` 时由 ``live_trades`` 重放，而现金分红
    不是成交、不在其中——不落台账重启后分红金额就丢了。**不走 ``live_cash_flows``**：
    该表金额全部计入 ``external_net_inflow``，会把分红当外部入金从净值里减掉。

    唯一约束即幂等键：同一会话同一组合同一事件只应用一次（重复调用不重复加钱）。
    """
    __tablename__ = "live_corporate_actions"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"), nullable=False)
    portfolio_strategy_id = Column(Integer, nullable=False)
    code = Column(String(20), nullable=False)
    event_type = Column(String(16), nullable=False)
    ex_date = Column(Date, nullable=False)
    # 实际生效日（事件日为非交易日时顺延到下一交易日）
    applied_date = Column(Date, nullable=False)
    # 入账现金（分红）；送转留 0
    amount = Column(Numeric(15, 2), default=0, server_default="0", nullable=False)
    # 送转股数变动
    qty_delta = Column(Integer, default=0, server_default="0", nullable=False)
    note = Column(String(200), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("live_session_id", "portfolio_strategy_id", "code",
                         "event_type", "ex_date",
                         name="uq_live_corporate_actions_applied"),
        Index("ix_live_corporate_actions_session", "live_session_id", "applied_date"),
    )
