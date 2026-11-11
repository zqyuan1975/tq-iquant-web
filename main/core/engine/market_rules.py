"""市场交易规则（回测/实盘共用）。

涨跌停幅度口径回测与实盘必须一致：回测 Portfolio 的涨跌停延后判定与实盘
TdxTqDispatcher 的下单前置校验共用同一函数，避免两处口径漂移。

组合交易时段（trading_session）闸同属两端口径：回测按 bar 时间、实盘按
墙钟判定，允许窗口在此统一定义（market_rules 与涨跌停幅度同级的共享规则）。
"""
from datetime import date, time as dtime
from decimal import Decimal
from typing import Optional

from .price_limit import (
    BOARD_LIMIT_RATIOS,
    LimitRule,
    resolve_limit_rule,
)

#: resolve_limit_rule 给出「无可靠幅度」（ETF/转债/未知/新股窗口）时的回测兜底幅度。
#: 回测没有柜台兜底，必须有一个有限幅度才能判定涨跌停；保持旧口径 10%（宁宽勿错），
#: 不引入「跳过校验」的行为反转。
_FALLBACK_LIMIT_RATIO = Decimal("0.10")


def limit_rule_for(
    stock_code: str,
    *,
    name: Optional[str] = None,
    listed_bars: Optional[int] = None,
    earliest_bar_date: Optional[date] = None,
    today: Optional[date] = None,
) -> LimitRule:
    """回测/桥兜底用的「有限幅度」涨跌停规则：**复用实盘同一份 resolve_limit_rule**。

    实盘 tdx/ths 通道用 ``resolve_limit_rule`` 推导（ETF/转债/新股窗口 fail-open）；
    回测没有柜台兜底，``ratio=None`` 时回落到板块常规幅度（``BOARD_LIMIT_RATIOS``，
    无档位板块回落 10%），保证「同一只票、同一份规则」——ST 5%、创业板/科创板 20%、
    北交所 30% 与实盘逐值一致。``source`` 追加 ``_board_fallback`` 便于区分「确定档位」
    与「兜底档位」。

    回测无可靠上市日期数据，``listed_bars``/``earliest_bar_date`` 默认不传 →
    新股窗口不生效（按板块兜底，偏保守，不会误放行）。
    """
    rule = resolve_limit_rule(
        stock_code, name=name, listed_bars=listed_bars,
        earliest_bar_date=earliest_bar_date, today=today,
    )
    if rule.ratio is not None:
        return rule
    ratio = BOARD_LIMIT_RATIOS.get(rule.board, _FALLBACK_LIMIT_RATIO)
    return LimitRule(
        ratio=ratio,
        unlimited=False,
        board=rule.board,
        source="%s_board_fallback" % rule.source,
        confidence="low",
    )


def limit_ratio_for(
    stock_code: str,
    *,
    name: Optional[str] = None,
    listed_bars: Optional[int] = None,
    earliest_bar_date: Optional[date] = None,
    today: Optional[date] = None,
) -> Decimal:
    """涨跌停幅度（Decimal）：``limit_rule_for`` 的比例部分。"""
    return limit_rule_for(
        stock_code, name=name, listed_bars=listed_bars,
        earliest_bar_date=earliest_bar_date, today=today,
    ).ratio


def _limit_ratio_for(stock_code: str) -> Decimal:
    """兼容旧入口（无名称/上市信息）：等价于板块常规幅度。

    保留此函数是为了不破坏既有 import（回测 re-export、HTTP 桥兜底），
    新代码请用 ``limit_ratio_for``/``limit_rule_for`` 以便传入名称识别 ST。
    """
    return limit_ratio_for(stock_code)


def tick_size(stock_code: str) -> Decimal:
    """最小报价单位（撤单追价重下用，批次1 3a）：ETF 0.001，其余 0.01。

    ETF 代码前缀：沪 5 开头（51x/56x/58x 等）、深 1 开头（15x/16x 等）；
    其余（沪 6 / 深 0、3 / 北 4、8）按 0.01。
    """
    prefix = stock_code.split(".")[0]
    if prefix.startswith(("5", "1")):
        return Decimal("0.001")
    return Decimal("0.01")


# trading_session（组合表字段，full/am/pm）允许时段（上海时间，含端点）：
# am=09:30–11:30（早盘）、pm=13:00–15:00（午盘）、full=不筛（默认）。
TRADING_SESSION_WINDOWS = {
    "am": (dtime(9, 30), dtime(11, 30)),
    "pm": (dtime(13, 0), dtime(15, 0)),
}

# 时段闸仅对日内周期有意义：日线及以上 bar 的 bar_time 按约定为 00:00，
# 落在 am/pm 窗口之外——若不加区分，am/pm 组合的日线回测会被整程跳过、
# 实盘 am 组合的 14:30 日线驱动也永远进不来（历史陷阱，见 session_allows_bar）。
_INTRADAY_PERIODS = {"1m", "5m", "15m", "30m", "1h", "60m"}


def session_allows(session: Optional[str], t: dtime) -> bool:
    """组合交易时段闸（时间维度）：session 非 am/pm（含 None/full/未知值）一律放行。

    回测传 bar_time.time()、实盘传墙钟 now_shanghai().time()，判定同口径。
    bar 维度请用 session_allows_bar（含日内周期判定）。
    """
    window = TRADING_SESSION_WINDOWS.get(session or "")
    if window is None:
        return True
    return window[0] <= t <= window[1]


def session_allows_bar(session: Optional[str], t: dtime, period: Optional[str]) -> bool:
    """组合交易时段闸（bar 维度）：时段约束仅对日内周期生效。

    日线及以上周期（1d/1w/1mon，bar_time 约定 00:00）不受 am/pm 窗口过滤——
    否则 am/pm 组合的日线回测会被整程跳过、实盘 am 组合的 14:30 日线驱动
    永远进不来。日内 bar（及 period 未知的 bar，保守视为日内）按窗口判定
    （回测传 bar_time、实盘传墙钟）。
    """
    if period is not None and period not in _INTRADAY_PERIODS:
        return True
    return session_allows(session, t)


# ---- 板块每手规则（实盘下单/审批整手口径；科创板 200 股起购）----
# 来源：project2 版本 _MARKET_RULES 表校准。转债（10 张）由 lot_size_for 单独处理。
_LOT_SIZE_RULES: dict = {
    ("688", ".SH"): 200,   # 科创板：200 股起购（超出部分可 1 股递增，整手口径取 200）
}


def lot_size_for_board(stock_code: str) -> int:
    """按板块返回每手股数：科创板 200，其余 100（转债口径另由 lot_size_for 处理）。"""
    code = (stock_code or "").upper().strip()
    for (prefix, suffix), lot in _LOT_SIZE_RULES.items():
        if code.startswith(prefix) and code.endswith(suffix):
            return lot
    return 100
