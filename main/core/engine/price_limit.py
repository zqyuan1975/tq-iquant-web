"""涨跌停幅度推算（板块 + 标的状态），通达信 / 同花顺两条通道共用。

桥通道（`iquant` / `goldminer`）走 ``GET /price_limit`` 拿交易所真实值，不需要
本模块；通达信 TQ 与同花顺 UI 自动化没有该端点，只能本地推：**前收 × 幅度**。

旧实现一律按「板块固定比例」推（主板/ETF 10%、创业板/科创板 20%、北交所 30%），
以下情形必然推错：

- 主板 ST / *ST：幅度 5%，不是 10%（推宽 → 放行超限单，靠柜台兜底）；
- 创业板/科创板 ST：幅度仍 20%，不降档（按 10% 推窄 → 拦住合法单）；
- 新股上市初期：主板/创业板/科创板前 5 个交易日、北交所上市首日**无涨跌幅限制**
  （按比例推 → 拦住合法单）；
- 可转债（首日 57.3%/43.3%、次日起 20%）与 ETF（10%/20% 视标的）无法仅凭代码
  定档（按 10% 推 → 两种方向都可能错）。

判据来源（写清依据，便于回放时区分「推的」与「确定的」）：

1. 板块：证券代码前缀（沪深主板 000/001/002/003/600/601/603/605；创业板
   300/301/302；科创板 688/689；北交所 4xxxxx/8xxxxx/92xxxx）——确定性高
   （``confidence=high``，``source=board_default_*``）。
2. ST：证券**名称**里的 ST 标记（含 \*ST / SST）。名称取不到时不能假定非 ST，
   按板块默认幅度推并在 ``source`` 里标注 ``name_unknown``、``confidence=medium``
   ——主板 ST 会推成 10%（偏宽，fail-open 方向，柜台 5% 仍会拒）。
3. 上市交易日序：最近日线根数（= 上市以来已完成交易日数）。只有在「根数 < 窗口」
   且「最早一根日线距今 ≤ 45 个自然日」时才认定新股——后者防本地日线残缺
   （只缓存了几根就当成新股）。判定成功 ``confidence=medium``（根数受本地数据
   完整度影响），主板新股历史上有 44% 首日限价，故主板给 ``confidence=low``。

**取不到可靠依据时统一 fail-open（放行，交由柜台复核）**，理由：

- 通达信 / 同花顺柜台自身有涨跌停校验，超价委托必被交易所拒绝，放行不会产生
  真实超限成交；
- 本地误拦的代价更高：盘前 deferred 单被拦后不会自动重报，信号等于静默丢失；
- 与现有实现一致（取数失败/前收失真时本来就 fail-open），不引入行为反转。

所有分支都在 ``LimitRule.source`` / ``confidence`` 里留痕，通道侧写日志
（拦截文案同时带上 ``source``/``confidence``，随拒单进工作台决策事件）。
"""
import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional

logger = logging.getLogger(__name__)

#: 「bar 与昨收不自洽」一次性诊断去重键（进程级，按 (code, 原因) 只报一次）。
_BAR_INCONSISTENT_WARNED: set = set()

BOARD_MAIN = "main"
BOARD_CHINEXT = "chinext"
BOARD_STAR = "star"
BOARD_BSE = "bse"
BOARD_FUND = "fund"
BOARD_BOND = "bond"
BOARD_UNKNOWN = "unknown"

#: 各板块常规涨跌停幅度
BOARD_LIMIT_RATIOS = {
    BOARD_MAIN: Decimal("0.10"),
    BOARD_CHINEXT: Decimal("0.20"),
    BOARD_STAR: Decimal("0.20"),
    BOARD_BSE: Decimal("0.30"),
}

#: 各板块 ST/*ST 幅度（主板 5%，创业板/科创板不降档仍 20%，北交所 30%）
BOARD_ST_RATIOS = {
    BOARD_MAIN: Decimal("0.05"),
    BOARD_CHINEXT: Decimal("0.20"),
    BOARD_STAR: Decimal("0.20"),
    BOARD_BSE: Decimal("0.30"),
}

#: 上市初期无涨跌幅限制的交易日数：沪深（含主板注册制/创业板/科创板）前 5 个
#: 交易日；北交所仅上市首日。0 / 缺省 = 该板块不做新股特判。
NEW_LISTING_SESSIONS = {
    BOARD_MAIN: 5,
    BOARD_CHINEXT: 5,
    BOARD_STAR: 5,
    BOARD_BSE: 1,
}

#: 新股判定的辅助闸：最早一根日线距今超过该自然日数即视为「本地数据残缺」
#: （正常新股前 5 个交易日在长假前后也不超过 ~20 天，45 天留足余量）。
_NEW_LISTING_MAX_SPAN_DAYS = 45

#: 无法定档的板块（ETF/LOF、可转债、未知代码）：fail-open，不推价格
_UNRESOLVED_BOARDS = (BOARD_FUND, BOARD_BOND, BOARD_UNKNOWN)

#: TQ 批量信息接口里证券名称的候选字段名（各版本不统一，深度遍历时逐个匹配）
_NAME_KEYS = ("Name", "StockName", "证券名称", "名称")

#: 可转债/债券代码前缀（沪 110/111/113/118、深 123/127/128、可交换债 132、
#: 国债/企债 010/018/019/020/100/102/103/108/112/117/120）
_BOND_PREFIXES = (
    "110", "111", "112", "113", "117", "118", "120", "123", "127", "128", "132",
    "010", "018", "019", "020", "100", "102", "103", "108",
)


@dataclass(frozen=True)
class LimitRule:
    """涨跌停推算结果。

    ``ratio`` 为 None 表示**无可靠幅度**（新股无限制 / ETF / 转债 / 未知代码），
    调用方应跳过本地校验（fail-open，交柜台）。
    """

    ratio: Optional[Decimal]
    unlimited: bool
    board: str
    source: str
    confidence: str

    @property
    def label(self) -> str:
        """日志/拒单文案用的紧凑描述。"""
        ratio = "-" if self.ratio is None else str(self.ratio)
        return "board=%s ratio=%s source=%s confidence=%s" % (
            self.board, ratio, self.source, self.confidence)


def board_for(stock_code: str) -> str:
    """按 6 位代码前缀判板块（带不带后缀均可）。"""
    prefix = str(stock_code or "").split(".")[0].strip()
    digits = "".join(ch for ch in prefix if ch.isdigit())
    if len(digits) != 6:
        return BOARD_UNKNOWN
    if digits.startswith(("688", "689")):
        return BOARD_STAR
    if digits.startswith(("300", "301", "302")):
        return BOARD_CHINEXT
    # 北交所：4xxxxx / 8xxxxx / 92xxxx（新股段）；92 需先于 9 开头的 B 股判断
    if digits.startswith("92") or digits[0] in ("4", "8"):
        return BOARD_BSE
    if digits.startswith(_BOND_PREFIXES):
        return BOARD_BOND
    if digits[0] in ("5", "1"):
        # 5xx 沪市 ETF/LOF、1xx 深市 ETF/LOF（深市转债 12x 已在上一步判掉）
        return BOARD_FUND
    if digits[0] in ("0", "2", "3", "6", "9"):
        # 000/001/002/003 深主板；600/601/603/605 沪主板；200/900 B 股
        return BOARD_MAIN
    return BOARD_UNKNOWN


def is_st_name(name: Optional[str]) -> bool:
    """证券名称是否带 ST / *ST / SST 标记（名称取不到 → False，由 confidence 标注）。"""
    if not name:
        return False
    return "ST" in str(name).upper().replace(" ", "")


def extract_name(raw, symbol: str = "") -> Optional[str]:
    """从 TQ 批量信息/快照返回里尽力提取证券名称（字段名跨版本不统一）。

    只在命中名称字段或 ``symbol`` 节点下取值，不做「任意字符串当名称」的兜底——
    返回值形如 ``{"ErrorId": "0"}`` 时不能把 "0" 当成名称。
    """
    if isinstance(raw, dict):
        for key in _NAME_KEYS:
            value = raw.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for key in (symbol, str(symbol or "").split(".")[0]):
            if key and key in raw:
                found = extract_name(raw[key], symbol)
                if found:
                    return found
        for value in raw.values():
            if isinstance(value, (dict, list, tuple)):
                found = extract_name(value, symbol)
                if found:
                    return found
        return None
    if isinstance(raw, (list, tuple)):
        for item in raw:
            found = extract_name(item, symbol)
            if found:
                return found
    return None


def _is_new_listing(
    board: str, listed_bars: Optional[int],
    earliest_bar_date: Optional[date], today: Optional[date],
) -> bool:
    """是否处于「上市初期无涨跌幅限制」窗口。

    ``listed_bars`` = 截至上一交易日已完成的日线条数（= 上市后已交易天数）。
    缺任一判据（根数 / 最早 bar 日期 / 今日）一律不认定——宁可退回板块默认幅度。
    """
    window = NEW_LISTING_SESSIONS.get(board, 0)
    if not window or listed_bars is None:
        return False
    if earliest_bar_date is None or today is None:
        return False
    try:
        span = (today - earliest_bar_date).days
    except TypeError:
        return False
    if span < 0 or span > _NEW_LISTING_MAX_SPAN_DAYS:
        # 最早日线离今天太远 → 本地日线残缺，不是新股
        return False
    return listed_bars < window


def resolve_limit_rule(
    stock_code: str,
    name: Optional[str] = None,
    listed_bars: Optional[int] = None,
    earliest_bar_date: Optional[date] = None,
    today: Optional[date] = None,
) -> LimitRule:
    """按「板块 + ST 状态 + 上市交易日序」给出涨跌停幅度。

    ``ratio=None`` → 无可靠幅度，调用方跳过本地校验（fail-open，交柜台）。
    """
    board = board_for(stock_code)
    if board in _UNRESOLVED_BOARDS:
        return LimitRule(
            ratio=None, unlimited=True, board=board,
            source="board_%s_ratio_unresolved" % board, confidence="low")

    if _is_new_listing(board, listed_bars, earliest_bar_date, today):
        return LimitRule(
            ratio=None, unlimited=True, board=board,
            source="new_listing_%d_sessions" % NEW_LISTING_SESSIONS[board],
            # 主板历史上有 44% 首日限价，注册制之后才改为前 5 日无限制
            confidence="low" if board == BOARD_MAIN else "medium")

    if is_st_name(name):
        return LimitRule(
            ratio=BOARD_ST_RATIOS[board], unlimited=False, board=board,
            source="name_st", confidence="high")

    if name is None:
        # 名称取不到 → 无法排除 ST：按板块默认（偏宽）推，置信度降级。
        return LimitRule(
            ratio=BOARD_LIMIT_RATIOS[board], unlimited=False, board=board,
            source="board_default_name_unknown", confidence="medium")

    return LimitRule(
        ratio=BOARD_LIMIT_RATIOS[board], unlimited=False, board=board,
        source="board_default", confidence="high")


#: bar 自身价格相对「昨收 × 幅度」的越界容差：一手最小价差 0.01 元 + 千分之一。
BAR_CONSISTENCY_ABS_TOLERANCE = Decimal("0.011")
BAR_CONSISTENCY_REL_TOLERANCE = Decimal("0.001")


def check_bar_consistent_with_prev_close(
    *, code: str, high, low, prev_close, ratio,
) -> Optional[str]:
    """校验「当根 bar 的高/低」是否落在按昨收推的涨跌停区间内（P1-2）。

    用途：若昨收因公司行为未入账/复权口径错配而失真，则 ``prev_close × 幅度``
    推出来的涨跌停价本身是错的，夹价就会凭空多付/少收。这里**只做诊断**：

    - 返回 ``"high_above_limit"`` / ``"low_below_limit"`` / ``None``；
    - 绝不改调用方的夹价结果（fail-open，照旧按原规则夹）；
    - 同一 ``(code, 原因)`` 进程内只告警一次，避免逐日刷屏。
    """
    if ratio is None or prev_close is None:
        return None
    try:
        pc = Decimal(str(prev_close))
        if pc < Decimal("1"):
            return None
        hi = Decimal(str(high)) if high is not None else None
        lo = Decimal(str(low)) if low is not None else None
        up = pc * (1 + Decimal(str(ratio)))
        down = pc * (1 - Decimal(str(ratio)))
        up_tol = max(BAR_CONSISTENCY_ABS_TOLERANCE,
                     up * BAR_CONSISTENCY_REL_TOLERANCE)
        down_tol = max(BAR_CONSISTENCY_ABS_TOLERANCE,
                       down * BAR_CONSISTENCY_REL_TOLERANCE)
        reason = None
        if hi is not None and hi > up + up_tol:
            reason = "high_above_limit"
        elif lo is not None and lo < down - down_tol:
            reason = "low_below_limit"
        if reason is None:
            return None
        key = (str(code), reason)
        if key not in _BAR_INCONSISTENT_WARNED:
            _BAR_INCONSISTENT_WARNED.add(key)
            logger.warning(
                "bar 与昨收不自洽（%s，%s）：high=%s low=%s prev_close=%s "
                "ratio=%s → 涨跌停推断可能失真；仅诊断，不改夹价",
                code, reason, high, low, prev_close, ratio)
        return reason
    except Exception:  # noqa: BLE001 — 诊断绝不影响回测/实盘
        return None

