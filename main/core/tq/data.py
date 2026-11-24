import json
import logging

import pandas as pd
import polars as pl
from typing import Dict, List, Callable, Optional
from .utils import get_tdx_lock, get_tq, P2, current_priority, gate_config, parse_priority

logger = logging.getLogger(__name__)

_FIELDS = ["Open", "High", "Low", "Close", "Volume", "Amount"]

#: 平台取数默认复权口径 = **不复权（真实成交价）**。
#: 2026-11-22 口径决定：信号与记账统一用不复权原始价（公司行为按除权日显式入账），
#: 前复权/后复权降级为展示/对照。默认值在此集中定义，避免调用方漏传时静默漂回
#: 前复权（前复权因子随请求窗口变化，跨日持久化会产生假盈亏）。
DEFAULT_DIVIDEND_TYPE = "none"

# --- 午休幽灵 bar（13:00）---
# 批量 get_market_data(fill_data=True) 会把午休空档用「上一根 bar 收盘」填充：
# 在 11:30 后重复一根同样的数据并打 13:00 时间戳（1m/5m/15m/30m/1h 皆如此，
# 实测 30m = 9 根/日，13:00 与 11:30 的 OHLC 完全一致）。TQ 公式/通达信图表返回
# 逐 bar 真实序列，无此 bar；且 A 股分钟 bar 下午首根合法时间为
# 13:01/13:05/13:15/13:30/14:00，13:00 永远不可能真实存在，整根剔除安全。
# 不清掉会让 klines 时间轴多一根（9 根/日 vs 公式 8 根/日），分钟级公式输出按
# 索引对齐时整体前移——等于把后 N 根 bar 的信号全部提前触发（隐性未来函数）。
_LUNCH_PHANTOM_PERIODS = {"1m", "5m", "15m", "30m", "1h"}
_LUNCH_PHANTOM_TIME = "13:00"


def strip_lunch_phantom(raw: Optional[dict], period: str) -> Optional[dict]:
    """剔除 get_market_data(fill_data=True) 批量取数注入的午休幽灵 13:00 bar。

    raw: {field: pandas.DataFrame(index=时间戳, columns=股票代码)}。
    无 13:00（或非分钟周期，1d/1w/1mon 无此问题）时原样透传（对象同一，零拷贝）。
    """
    if not raw or period not in _LUNCH_PHANTOM_PERIODS:
        return raw
    out: Dict[str, pd.DataFrame] = {}
    changed = False
    for field, df in raw.items():
        if not isinstance(df, pd.DataFrame) or df.empty:
            out[field] = df
            continue
        mask = df.index.to_series().dt.strftime("%H:%M") != _LUNCH_PHANTOM_TIME
        cut = df.loc[mask]
        if cut.shape[0] != df.shape[0]:
            changed = True
            logger.debug("剔除 %s 午休幽灵 13:00 bar %d 根(field=%s)",
                         period, int(df.shape[0] - cut.shape[0]), field)
        else:
            cut = df
        out[field] = cut
    return out if changed else raw


def no_trade_bar_mask(
    opens: List, highs: List, lows: List, closes: List, volumes: List,
) -> List[bool]:
    """识别 ``fill_data=True`` 用上一根收盘补出的「无成交 bar」（停牌/上市前）。

    背景（2026-11-16 P0 核查）：``get_market_data(fill_data=True)`` 遇到停牌/未上市
    区间会用「上一根 bar 收盘」补行——OHLC 四价全等于前收、Volume=0，价格 >0，
    因此不会被「Open/Close<=0」的伪影过滤拦下。这类假 bar 一旦进入回测价格索引，
    停牌日照样成交、照估值、照推进日快照（假收益/假回撤/假换手）。

    判据（与审计口径一致，纯函数便于单测）：
      - ``Volume == 0`` 且四价完全相同（contains no trade）；
      - 且该价格等于**上一根有效 bar 收盘**（carry-forward，停牌中段），
        或无前收时等于**下一根有效 bar 收盘**（上市前回填）。
    真实一字板/低成交日的 Volume > 0，不会命中；指数行情（Volume 常为 0）
    不调用本函数（只在个股 klines 转换时使用）。

    返回与输入等长的布尔列表，True = 无成交 bar，应整行剔除。
    """
    n = len(closes)
    mask = [False] * n
    flat_vol0: List[bool] = []
    for i in range(n):
        o, h, l, c, v = opens[i], highs[i], lows[i], closes[i], volumes[i]
        try:
            is_flat = (
                o is not None and c is not None and o > 0
                and o == h and o == l and o == c
                and v is not None and v == 0
            )
        except TypeError:
            is_flat = False
        flat_vol0.append(bool(is_flat))

    prev_close = None
    for i in range(n):
        if flat_vol0[i] and prev_close is not None and closes[i] == prev_close:
            mask[i] = True
        if not flat_vol0[i] and closes[i] is not None and closes[i] > 0:
            prev_close = closes[i]

    next_close = None
    for i in range(n - 1, -1, -1):
        if flat_vol0[i] and not mask[i] and next_close is not None \
                and closes[i] == next_close:
            mask[i] = True
        if not flat_vol0[i] and not mask[i] and closes[i] is not None and closes[i] > 0:
            next_close = closes[i]
    return mask


class TQData:
    def __init__(self):
        # 最近一次 get_history_raw 因行情数据异常被跳过的股票代码。
        # 通达信偶发对个别股票返回非法/占位日期（如 '20040000'）会让批量
        # get_market_data 整批抛错；逐股兜底时把取不到的股票记在这里，
        # 供上层（build_klines/run_backtest）转成回测告警。
        self.skipped_stocks: List[str] = []

    def get_stock_pools(self) -> List[dict]:
        with get_tdx_lock():
            return self._get_pools()

    def get_pool_stocks(self, pool_code: str) -> List[dict]:
        with get_tdx_lock():
            return self._get_stocks(pool_code)

    def get_industry_list(self) -> List[dict]:
        """通达信二级行业（881xxx，共 128 个），归一化 [{"code","name"}]。

        list_type=1 时 get_sector_list 返回 [{"Code","Name"}] 字典。
        """
        with get_tdx_lock():
            raw = get_tq().get_sector_list(list_type=1) or []
            out: List[dict] = []
            for item in raw:
                if not isinstance(item, dict):
                    continue
                code = str(item.get("Code") or "")
                if code.startswith("881"):
                    out.append({"code": code,
                                "name": str(item.get("Name") or code)})
            return out

    def get_stock_industry(self, stock_code: str) -> Optional[dict]:
        """个股所属通达信二级行业（get_relation 中 BlockType=='行业' 的唯一块）。

        返回 {"code": "881002.SH", "name": "煤炭开采"}；无行业块返回 None。
        """
        with get_tdx_lock():
            blocks = get_tq().get_relation(stock_code) or []
            for b in blocks:
                if isinstance(b, dict) and b.get("BlockType") == "行业":
                    return {"code": b.get("BlockCode"),
                            "name": b.get("BlockName")}
            return None

    # --- TQ 1.3.1 轻量包装（试算/交易日历/涨跌停/自选股/退订/除权/批量） ---
    # 最薄透传：不包大 try/except，fail-open 由调用方负责；仅 SDK 打印型失败
    # 或测试契约要求的场景返回安全默认。

    def get_trading_dates(
        self, market: str = "SH", start: str = "", end: str = "",
        strict: bool = False,
    ) -> List[str]:
        """交易日历，归一化为 ["YYYYMMDD", ...] 字符串列表。

        探针实测（1.3.1）：market 传 'SH'（'SZ'/'1'/'0'/'' 均被 SDK 宽松接受，
        返回同样结果）。SDK 失败打印并返回 []，异常默认归一为空列表；
        strict=True 时异常原样 re-raise（REST 层据此区分失败与真空，
        query_calendar 消费保持默认非 strict 不变）。
        """
        try:
            with get_tdx_lock():
                raw = get_tq().get_trading_dates(
                    market=market, start_time=start, end_time=end) or []
            return [str(d).strip() for d in raw if str(d).strip()]
        except Exception:
            if strict:
                raise
            return []

    def get_zdt_data(self, stocks: List[str]) -> dict:
        """涨跌停状态：{code: {ZDTStatusNow/ZDTStatusOri/FirstTimeZT/DT, ...}}。

        仅状态无价格，原样透传 SDK 的 Value，不做价格推断；空列表短路不触 SDK。
        """
        if not stocks:
            return {}
        with get_tdx_lock():
            return get_tq().get_zdt_data(list(stocks)) or {}

    def ensure_sector(self, block_code: str, block_name: str) -> dict:
        """创建自定义板块（已存在时 SDK 端不报错，返回带 ErrorId 的结果）。

        SDK create_sector 返回 JSON 字符串（失败 None），解析为 dict 供调用方判断。
        """
        with get_tdx_lock():
            raw = get_tq().create_sector(block_code=block_code, block_name=block_name)
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str) and raw.strip():
            try:
                return json.loads(raw)
            except ValueError:
                return {}
        return {}

    def delete_sector(self, block_code: str):
        """删除自定义板块（配合 send_user_block 的追加语义实现覆盖写）。

        薄透传：SDK delete_sector 返回 JSON 字符串或 dict（失败 None），原样
        返回不解析——结果处理（best-effort 忽略）由调用方负责。
        """
        with get_tdx_lock():
            return get_tq().delete_sector(block_code=block_code)

    def send_user_block(
        self, block_code: str, stocks: List[str], show: bool = False,
    ) -> dict:
        """添加自选股到板块（show=True 时客户端展示该板块）。"""
        with get_tdx_lock():
            return get_tq().send_user_block(
                block_code=block_code, stock_list=list(stocks), show=show) or {}

    def unsubscribe_bars(self, stocks: List[str]) -> None:
        """退订行情（unsubscribe_hq）。空列表直接返回不触 SDK——SDK 对空列表
        raise ValueError 并 close 连接，绝不能让它发生。"""
        if not stocks:
            return
        with get_tdx_lock():
            get_tq().unsubscribe_hq(list(stocks))

    def get_divid_factors(
        self, stock_code: str, start: str = "", end: str = "",
    ) -> pd.DataFrame:
        """除权除息数据；空代码短路返回空 DataFrame（SDK 同样返回空表）。"""
        if not stock_code:
            return pd.DataFrame()
        with get_tdx_lock():
            df = get_tq().get_divid_factors(stock_code, start_time=start, end_time=end)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

    def get_market_snapshot_batch(
        self, stocks: List[str], field_list: List[str],
        priority=None, timeout: Optional[float] = None,
    ) -> dict:
        """批量市场快照（备用透传，return_df=False 默认 dict）。

        ``priority`` / ``timeout`` 供保护窗分钟合成兜底使用（P3 + 短超时）：
        不传时行为与接入前逐字一致（默认 P1、默认 30s 超时）。
        """
        with get_tdx_lock(timeout=timeout, priority=priority):
            return get_tq().get_market_snapshot_batch(
                stock_list=list(stocks), field_list=list(field_list)) or {}

    def get_stock_info_batch(self, stocks: List[str], field_list: List[str]) -> dict:
        """批量证券基础信息（备用透传，return_df=False 默认 dict）。"""
        with get_tdx_lock():
            return get_tq().get_stock_info_batch(
                stock_list=list(stocks), field_list=list(field_list)) or {}

    def get_more_info_batch(self, stocks: List[str], field_list: List[str]) -> dict:
        """批量股票更多信息（备用透传，return_df=False 默认 dict）。"""
        with get_tdx_lock():
            return get_tq().get_more_info_batch(
                stock_list=list(stocks), field_list=list(field_list)) or {}

    # --- 条件选股取数（2026-11-24，A0-1）------------------------------------
    # 三个方法都只做「薄透传 + P2 分片」，不在这里做 PIT 截断、单位换算或缺失判定：
    # 财报必须由调用方按 announce_time ≤ 决策日逐只逐期切片，否则重述会泄漏未来信息。
    # 分片语义与 get_history_raw 一致：逐片持闸、片间放锁；任一片异常即整体抛出，
    # 不做「部分成功」的静默降级——财务少一片等于口径不全，必须失败可见。

    def _selection_chunk(self, level: int, n: int) -> int:
        """P2 且票数超阈值时返回分片大小，否则 0（不分片）。"""
        cfg = gate_config()
        try:
            chunk = int(cfg.get("p2_chunk_size") or 0)
        except (TypeError, ValueError):
            chunk = 0
        if level == P2 and cfg.get("enabled", True) and chunk > 0 and n > chunk:
            return chunk
        return 0

    def _selection_fetch(self, fetch, stocks, level: int, chunk: int) -> pd.DataFrame:
        """按片调用 ``fetch(part)`` 并纵向合并；空片跳过，全空返回空表。"""
        if chunk <= 0:
            with get_tdx_lock(priority=level):
                df = fetch(list(stocks))
            return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
        frames = []
        for i in range(0, len(stocks), chunk):
            part = list(stocks[i:i + chunk])
            with get_tdx_lock(priority=P2):
                df = fetch(part)
            if isinstance(df, pd.DataFrame) and not df.empty:
                frames.append(df)
        if not frames:
            return pd.DataFrame()
        return frames[0] if len(frames) == 1 else pd.concat(frames, axis=0)

    def get_financial_data(
        self, stocks: List[str], table_list: List[str],
        start_time: str = "", report_type: str = "report_time", priority=None,
    ) -> pd.DataFrame:
        """批量财报序列（FN 字段 × 报告期）。

        ``report_type="report_time"`` 按公告日返回；结果同时带 tag_time /
        announce_time 平行数组，PIT 截断由调用方完成。
        """
        codes = [c for c in (stocks or []) if c]
        if not codes or not table_list:
            return pd.DataFrame()
        level = current_priority() if priority is None else parse_priority(priority)
        chunk = self._selection_chunk(level, len(codes))
        return self._selection_fetch(
            lambda part: get_tq().get_financial_data(
                stock_list=part, table_list=list(table_list),
                start_time=start_time, report_type=report_type),
            codes, level, chunk)

    def get_gpjy_value_by_date(
        self, stocks: List[str], year: int, mmdd: str,
        table_list: List[str], priority=None,
    ) -> pd.DataFrame:
        """按「年 + 月日」定位取历史逐日指标（如 GP16 = 当日总市值，万元）。

        一次只取一天；跨日/跨年由调用方循环，本方法只负责单日批量 + P2 分片。
        """
        codes = [c for c in (stocks or []) if c]
        if not codes or not table_list:
            return pd.DataFrame()
        level = current_priority() if priority is None else parse_priority(priority)
        chunk = self._selection_chunk(level, len(codes))
        return self._selection_fetch(
            lambda part: get_tq().get_gpjy_value_by_date(
                stock_list=part, year=year, mmdd=mmdd, table_list=list(table_list)),
            codes, level, chunk)

    def get_gp_one_data(
        self, stocks: List[str], table_list: List[str], priority=None,
    ) -> pd.DataFrame:
        """批量一致预期（GO 字段）。只有 T/T+1/T+2 三个时点，没有历史序列。"""
        codes = [c for c in (stocks or []) if c]
        if not codes or not table_list:
            return pd.DataFrame()
        level = current_priority() if priority is None else parse_priority(priority)
        chunk = self._selection_chunk(level, len(codes))
        return self._selection_fetch(
            lambda part: get_tq().get_gp_one_data(
                stock_list=part, table_list=list(table_list)),
            codes, level, chunk)

    def get_history(
        self, stocks: List[str], periods: List[str],
        start: str = "", end: str = "",
        dividend_type: str = DEFAULT_DIVIDEND_TYPE, count: int = 100,
    ) -> Dict[str, Dict[str, pl.DataFrame]]:
        with get_tdx_lock():
            return self._load_history(stocks, periods, start, end, dividend_type, count)

    def get_history_raw(
        self, stocks: List[str], periods: List[str],
        start: str = "", end: str = "",
        dividend_type: str = DEFAULT_DIVIDEND_TYPE, count: int = 100,
        priority=None,
    ) -> Dict[str, dict]:
        """取原始 TQ 行情（未经转换），按周期分组：{period: {field: pandas.DataFrame}}。

        field ∈ Open/High/Low/Close/Volume/Amount，DataFrame.index=时间戳，columns=股票代码。
        一次调用 = 单周期多股票。转换由调用方（_convert_market_data）负责，便于单测。

        P2（后台重任务）且股票数超过 ``tq_gate.p2_chunk_size`` 时**按票分片**：
        每片一次独立的加锁/放锁，片间 P0 实盘调用可插队——避免一次
        ``count=-1`` 全量取数长时间独占通道（2026-11-19 实测 >2 分钟）。
        """
        level = current_priority() if priority is None else parse_priority(priority)
        cfg = gate_config()
        try:
            chunk = int(cfg.get("p2_chunk_size") or 0)
        except (TypeError, ValueError):
            chunk = 0
        if level == P2 and cfg.get("enabled", True) and chunk > 0 and len(stocks) > chunk:
            return self._history_raw_chunked(
                stocks, periods, start, end, dividend_type, count, chunk)
        with get_tdx_lock(priority=level):
            return self._load_history_raw(stocks, periods, start, end, dividend_type, count)

    def _history_raw_chunked(
        self, stocks, periods, start, end, dividend_type, count, chunk,
    ) -> Dict[str, dict]:
        """P2 分片取数：逐片持闸，片间放闸；按字段 ``pd.concat(axis=1)`` 合并。

        合并口径与批量一次取数一致（同构 {period: {field: DataFrame}}，列=股票），
        列序保持入参 ``stocks`` 顺序；某片异常时沿用 ``_load_history_raw`` 的逐股兜底
        与 ``skipped_stocks`` 语义，最终一次性回填。
        """
        merged: Dict[str, dict] = {}
        skipped: List[str] = []
        for i in range(0, len(stocks), chunk):
            part = stocks[i:i + chunk]
            with get_tdx_lock(priority=P2):
                one = self._load_history_raw(
                    part, periods, start, end, dividend_type, count)
            skipped.extend(self.skipped_stocks)
            for period, fields in (one or {}).items():
                bucket = merged.setdefault(period, {})
                for field, df in (fields or {}).items():
                    if df is None:
                        continue
                    prev = bucket.get(field)
                    bucket[field] = df if prev is None else pd.concat([prev, df], axis=1)
        self.skipped_stocks = skipped
        return merged

    def subscribe_bars(
        self, stocks: List[str], periods: List[str],
        callback: Callable,
    ) -> None:
        with get_tdx_lock():
            self._subscribe(stocks, periods, callback)

    def get_all_stocks(self, market: str = "5") -> List[str]:
        tq = get_tq()
        # tqcenter 非线程安全：与回测 worker 其它 SDK 调用同锁串行。
        with get_tdx_lock():
            return tq.get_stock_list(market) or []

    # --- TDX 底层调用 ---
    def _get_pools(self) -> List[dict]:
        """通达信用户自定义板块，归一化为 [{"code","name"}]。

        SDK get_user_sector() 返回 [{"Code","Name"}]（大写首字母）。
        v1 曾用 get_sector_list（系统板块，587 个，非用户板块）且把 dict 当 str → 线上炸。
        """
        # 调用方（get_stock_pools）已持 get_tdx_lock()。此处不能再取——
        # _tdx_lock 是非重入 threading.Lock，同线程重入即自死锁（股票池接口整路卡死，
        # 且锁永不释放会连带阻塞回测/实盘所有 TQ 调用）。私有 _load_history 等
        # 同口径均假设调用方持锁。
        tq = get_tq()
        sectors = tq.get_user_sector() or []
        return [{"code": s["Code"], "name": s["Name"]} for s in sectors]

    def _get_stocks(self, pool_code: str) -> List[dict]:
        """板块成分股，归一化为 [{"stock_code","stock_name"}]。

        SDK get_stock_list_in_sector(code, block_type=1, list_type=1) 返回
        [{"Code","Name"}]。必须传板块 Code（如 TQCS），传 Name 返回空。
        """
        # 同上：调用方（get_pool_stocks）已持锁，此处不可重入取锁。
        tq = get_tq()
        stocks = tq.get_stock_list_in_sector(
            pool_code, block_type=1, list_type=1) or []
        return [
            {"stock_code": s["Code"], "stock_name": s["Name"]}
            for s in stocks
        ]

    def _load_history(
        self, stocks, periods, start="", end="",
        dividend_type=DEFAULT_DIVIDEND_TYPE, count=100,
    ) -> Dict[str, Dict[str, pl.DataFrame]]:
        tq = get_tq()
        result = {}
        for period in periods:
            df = tq.get_market_data(
                field_list=["Open", "High", "Low", "Close", "Volume", "Amount"],
                stock_list=stocks,
                period=period,
                start_time=start,
                end_time=end,
                count=count,
                dividend_type=dividend_type,
                fill_data=True,
            )
            if df is not None:
                df = strip_lunch_phantom(df, period)
                for code in stocks:
                    if code not in result:
                        result[code] = {}
                    try:
                        series = {}
                        for col in ["Open", "High", "Low", "Close", "Volume", "Amount"]:
                            if col in df and code in df[col].columns:
                                series[col] = df[col][code]
                        if series:
                            # 无成交 bar（fill_data 补出的停牌/上市前假行）整行剔除：
                            # 见 no_trade_bar_mask 的判据与依据。
                            frame = pl.DataFrame(series)
                            if "Volume" in series:
                                mask = no_trade_bar_mask(
                                    series["Open"].to_list(),
                                    series["High"].to_list(),
                                    series["Low"].to_list(),
                                    series["Close"].to_list(),
                                    series["Volume"].to_list(),
                                )
                                if any(mask):
                                    frame = frame.filter(
                                        pl.Series([not m for m in mask]))
                            result[code][period] = frame
                    except Exception as e:
                        # 静默丢弃会让回测/试算少一只股票且不可复现——异常必须可见。
                        logger.warning(
                            "跳过行情构建失败股票 %s(period=%s)：%s", code, period, e)
        return result

    def _load_history_raw(
        self, stocks, periods, start="", end="",
        dividend_type=DEFAULT_DIVIDEND_TYPE, count=100,
    ) -> Dict[str, dict]:
        """原始 TQ 行情按周期分组：{period: raw_dict}。raw_dict 即 tq.get_market_data 返回值。

        批量调用若抛异常（通达信偶发返回非法日期，如 '20040000'），退化为逐只
        拉取并跳过取不到的股票，避免个别坏数据导致整单回测失败。被跳过的股票
        记录在 self.skipped_stocks。
        """
        tq = get_tq()
        self.skipped_stocks = []
        result: Dict[str, dict] = {}
        for period in periods:
            try:
                df = tq.get_market_data(
                    field_list=_FIELDS,
                    stock_list=stocks,
                    period=period,
                    start_time=start,
                    end_time=end,
                    count=count,
                    dividend_type=dividend_type,
                    fill_data=True,
                )
            except Exception as e:
                logger.warning(
                    "get_market_data 批量拉取失败(period=%s)，逐股兜底：%s", period, e)
                df = self._fetch_per_stock(
                    tq, stocks, period, start, end, dividend_type, count)
            if df is not None:
                result[period] = strip_lunch_phantom(df, period)
        return result

    def _fetch_per_stock(
        self, tq, stocks, period, start, end, dividend_type, count,
    ) -> Optional[dict]:
        """批量失败时逐只拉取，合并为与批量同构的 {field: DataFrame(index=time, columns=stocks)}。

        抛异常的股票记入 self.skipped_stocks 并跳过；全部失败返回 None。
        """
        collected: Dict[str, List[pd.DataFrame]] = {}
        for code in stocks:
            try:
                one = tq.get_market_data(
                    field_list=_FIELDS,
                    stock_list=[code],
                    period=period,
                    start_time=start,
                    end_time=end,
                    count=count,
                    dividend_type=dividend_type,
                    fill_data=True,
                )
            except Exception as e:
                self.skipped_stocks.append(code)
                logger.warning("跳过行情异常股票 %s(period=%s)：%s", code, period, e)
                continue
            if not isinstance(one, dict):
                continue
            for field in _FIELDS:
                sdf = one.get(field)
                if isinstance(sdf, pd.DataFrame) and sdf.shape[1] > 0:
                    collected.setdefault(field, []).append(sdf)
        if not collected:
            return None
        return {
            field: (dfs[0] if len(dfs) == 1 else pd.concat(dfs, axis=1))
            for field, dfs in collected.items()
        }

    def _subscribe(self, stocks, periods, callback):
        tq = get_tq()
        tq.subscribe_hq(stock_list=stocks, callback=callback)
