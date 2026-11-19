"""外部信号注入源 — 通达信条件预警文本文件信号源（0011 外部信号注入链路）。

通达信客户端「条件预警」可配置公式触发时动态输出文本文件（GBK 编码、CRLF、
追加写入、制表符分隔，每行一条，行尾常带一个多余 TAB）：

    当前版本（真机样本 BDXH.txt，无行号列，7 列 + 行尾 TAB）：
    300456\\t赛微电子\\t2026-09-08 23:08\\t37.61\\t-2.77%\\t 5387\\t波段加仓\\t

    早期版本（带行号列，8 列，保留兼容）：
    1\\t920718\\t六合高科\\t2026-10-30 09:50\\t 9.83\\t-1.90%\\t    1\\t分时突击

平台定时增量读取（时间戳游标 + 已消费行数 + lookback 时间窗口），公式名匹配
策略后注入 signal_cache 走 Portfolio.on_bar 全套（风控/审批/在途单闸不绕过）。
所有周期公式信号均可经此链路注入——包括 1m（绕过 TQ 公式引擎 1m 返回全空的
限制）。

时间窗口模型（批次1 外部信号改造）：通达信条件预警启动时会**补齐全部历史信号**
写入文件——按行号游标处理会消费所有旧信号（旧信号股价已拉升/条件已消失，
不应交易）。改为双重过滤：

    ① 时间戳游标（_last_read_time，初始=启动时当前时间）——只处理时间 >= 游标
       的行，启动前存量（含窗口内近端补齐行）全挡；游标时间戳上再记「已消费
       行数」（_cursor_count），同一分钟内晚到的第二/第 N 行不会被 <= 判定
       静默丢弃（见 read_new_signals）；
    ② lookback 窗口（默认 300s）——晚到的陈旧行（时间 > 游标但早于
       now - lookback_seconds）不返回，信号新鲜度兜底。

本源是通道无关的：文件由外部（通达信客户端）产生，平台只读，不回写不删除。
"""
import logging
import os
import re
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# 布局探测结果 = (代码列, 名称列, 时间列, 价格列, 公式名列, 最少列数)
# 当前版本：无行号列（真机样本 300456\t赛微电子\t2026-09-08 23:08\tt...）
_LAYOUT_PLAIN = (0, 1, 2, 3, 6, 7)
# 早期版本：首列行号、次列代码（8 列）
_LAYOUT_LEGACY = (1, 2, 3, 4, 7, 8)
_CODE_RE = re.compile(r"^\d{6}(?:\.\w{2})?$")
_TIME_FORMATS = (
    "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S",
)
# 单次读取上限：正常预警文件全天不过数百 KB；超过该值只读尾部（防误配成日志
# 等大文件时每 5s 全量读盘）。尾部首个半行丢弃。
_MAX_READ_BYTES = 4 * 1024 * 1024
# 坏行告警去重上限（独立轮询 5s 一次，不去重会刷屏）
_MAX_WARNED_LINES = 50


def _exchange_for(code: str) -> str:
    """6 位代码推断交易所后缀（与 tdx_tq_dispatcher._exchange_for / live_service
    _normalize_external_symbol 同口径：92→BJ（北交所新股段）、11→SH（沪可转债）、
    6/5/9→SH、4/8→BJ、其余→SZ）。"""
    if code.startswith("92"):
        return "BJ"
    if code.startswith("11"):
        return "SH"
    if code.startswith(("6", "5", "9")):
        return "SH"
    if code.startswith(("4", "8")):
        return "BJ"
    return "SZ"


def normalize_external_symbol(code: str) -> str:
    """预警文件 6 位代码 → 平台统一带后缀格式（920718 → 920718.BJ）。

    已带后缀原样返回；非 6 位数字且无后缀返回空串（调用方丢弃该行）。
    """
    text = str(code or "").strip()
    if "." in text:
        return text
    code6 = re.sub(r"\D", "", text)[:6]
    if len(code6) != 6:
        return ""
    return "%s.%s" % (code6, _exchange_for(code6))


class ExternalSignalFileSource:
    """通达信条件预警文本文件信号源。

    定时读取预警文件 → 解析（时间戳游标 + lookback 窗口增量）→ 返回新信号列表。
    文件由通达信客户端条件预警功能动态输出（追加写入），游标语义适配增量：
    每轮只消费晚于游标的时间窗口内新行，旧行不再重放。公式名不在
    strategy_formula_names 集合中的行不返回（文件里可能混有其它公式的预警）。
    """

    def __init__(
        self,
        file_path: str,
        strategy_formula_names: Set[str],
        lookback_seconds: int = 300,
        clock: Optional[Callable[[], datetime]] = None,
    ):
        self._path = file_path
        self._formula_names = set(strategy_formula_names or ())
        self._lookback_seconds = max(0, int(lookback_seconds))
        self._clock = clock or datetime.now
        # 时间戳游标：初始 = 启动时当前时间。通达信条件预警启动会补齐全部历史
        # 信号写入文件，游标把启动前存量（含窗口内近端补齐行）全部挡掉，
        # 只消费启动后新触发的信号。
        self._last_read_time: datetime = self._clock()
        # 游标时间戳上的「已消费行数」：文件只追加、每轮全量重读，同一分钟内
        # 晚到的第二行若只按时间（<= 游标）判定会被静默丢弃。改为：时间 == 游标
        # 的行按出现顺序跳过前 _cursor_count 条（已消费），其余全是新行。
        self._cursor_count: int = 0
        # 文件缺失告警去重：只在「存在 → 缺失」状态跳变时打一条 WARNING，
        # 避免每轮 tick 刷屏，同时让填错路径可见（不再静默当无信号）。
        self._missing_warned = False
        # 无变化跳过：独立轮询 5s 一次，size+mtime_ns 未变则不读盘不解析。
        self._last_stat: Optional[Tuple[int, int]] = None
        # 坏行告警去重（同一行同一原因只告警一次，防 5s 轮询刷屏）
        self._warned_lines: Set[Tuple[str, str]] = set()

    def read_new_signals(self) -> List[Dict]:
        """增量读取新信号行，解析为标准化信号 dict 列表。

        返回: [{"code": "920718", "name": "六合高科", "time": datetime,
                "price": Decimal("9.83"), "formula_name": "分时突击"}]
        （code 为文件中的 6 位原码，带后缀归一由引擎注入时完成。）

        双重过滤（批次1）：
        ① 时间戳游标——时间 < _last_read_time 的行已消费；时间 == 游标时按
           出现顺序跳过前 _cursor_count 条（同一分钟内更早读到的行），其余
           视为新行（同一分钟晚到的第二/第 N 行不再丢失）；
        ② lookback 窗口——只返回 signal_time >= now - lookback_seconds 的信号。
        游标推进：本轮文件全部可解析行中最晚 signal_time（单调不回退，防补齐
        旧行把游标拉回过去），同时记下该时间戳上的行数。编码 GBK（通达信
        输出）。解析失败的行跳过 + warning 留痕。

        去重不在本层做：文件不含买卖方向（方向由策略公式信号配置决定），
        同代码同日可能先出买入、后出卖出行。引擎按「同代码 + 同日 + 同方向」
        去重（默认各认第一条，可由配置放开），本层只保证「不重读已消费的行」。
        """
        now = self._clock()
        cutoff = now - timedelta(seconds=self._lookback_seconds)
        lines = self._read_complete_lines()
        if lines is None:
            return []  # 文件 size+mtime 未变：本轮跳过读盘与解析
        parsed: List[Dict] = []
        for raw in lines:
            line = raw.strip().lstrip("﻿").strip()
            if not line:
                continue  # 空行
            sig = self._parse_line(line)
            if sig is not None:
                parsed.append(sig)
        # 游标时间戳及之后每个时间戳的行数：同一分钟多行靠「已消费行数」区分新旧
        counts: Dict[datetime, int] = {}
        latest = self._last_read_time
        for sig in parsed:
            ts = sig["time"]
            if ts < self._last_read_time:
                continue
            counts[ts] = counts.get(ts, 0) + 1
            if ts > latest:
                latest = ts
        signals: List[Dict] = []
        seen_at_cursor = 0
        same_minute_rows = 0
        for sig in parsed:
            ts = sig["time"]
            if ts < self._last_read_time:
                continue  # ① 早于游标：已消费（含启动前存量行）
            if ts == self._last_read_time:
                seen_at_cursor += 1
                if seen_at_cursor <= self._cursor_count:
                    continue  # 该时间戳此前的行已消费过（全量重读去重）
                # ② lookback 窗口：晚到的陈旧行不交易（股价已拉升/条件已消失）
                if ts < cutoff:
                    continue
                signals.append(sig)
                same_minute_rows += 1
                continue
            # ② 时间晚于游标的新行同样受 lookback 新鲜度约束
            if ts < cutoff:
                continue
            signals.append(sig)
        if same_minute_rows:
            logger.info(
                "external signal same-minute row consumed: %d row(s) at %s "
                "(file %s)",
                same_minute_rows,
                self._last_read_time.strftime("%Y-%m-%d %H:%M"),
                self._path,
            )
        # 游标单调推进 = 文件中最晚信号时间；行数 = 该时间戳在文件中的总行数
        # （含本轮被 lookback 挡掉的，避免下一轮把它们当新行再判一次）。
        self._last_read_time = latest
        self._cursor_count = counts.get(latest, 0)
        return signals

    def reset_daily(self):
        """每日重置：时间戳游标归位当前时间（方向去重集合由引擎维护并同步清空）。

        供调用方在新交易日（预警文件按日轮转/换新文件路径）调用；游标归位
        当前时间，与「启动后新信号才消费」同语义——新文件的行只要时间晚于
        重置时点且落在 lookback 窗口内即消费。
        """
        self._last_read_time = self._clock()
        self._cursor_count = 0
        # 强制下一轮重读：文件内容未变（size+mtime 同）也要重新按新游标判定
        self._last_stat = None

    # ------------------------------------------------------------------
    # 只读状态（GET 会话详情的 signal_source 用；不暴露可变内部状态）
    @property
    def path(self) -> str:
        return self._path

    @property
    def formula_names(self) -> Set[str]:
        return set(self._formula_names)

    @property
    def last_read_time(self) -> datetime:
        return self._last_read_time

    @property
    def cursor_rows(self) -> int:
        return self._cursor_count

    # ------------------------------------------------------------------
    def _read_complete_lines(self) -> Optional[List[str]]:
        """读文件并按 \\n 切行。编码 GBK；末段无换行符视为通达信正在写半行，
        不消费（待补全后下一轮读出）。文件不存在/读失败 → 空列表（fail-closed：
        无信号即不下单）。

        返回 None = size+mtime_ns 与上轮一致，调用方跳过解析（5s 轮询下绝大多数
        轮次无新行，省掉读盘与整文件解析）。文件超过 _MAX_READ_BYTES 只读尾部，
        并丢弃首个不完整行。
        """
        try:
            with open(self._path, "rb") as f:
                st = os.fstat(f.fileno())
                size = st.st_size
                stat_key = (size, st.st_mtime_ns)
                prev_size = self._last_stat[0] if self._last_stat else 0
                if self._last_stat is not None and stat_key == self._last_stat:
                    return None
                if self._last_stat is not None and size < prev_size:
                    # 文件被截断/重建（通达信重启换文件、手工清空等）：游标保留，
                    # 早于游标的行不再消费——宁漏不重（重放旧信号会重复下单）。
                    logger.warning(
                        "外部信号文件被截断/重建（%d → %d 字节），保留时间戳游标，"
                        "仅消费游标之后的新行：%s",
                        prev_size, size, self._path,
                    )
                if size > _MAX_READ_BYTES:
                    self._warn_bad_line(
                        "size_cap",
                        "外部信号文件超过单次读取上限（%d 字节），仅读尾部 %d 字节：%s"
                        % (size, _MAX_READ_BYTES, self._path),
                    )
                    f.seek(size - _MAX_READ_BYTES)
                    raw = f.read()
                    nl = raw.find(b"\n")
                    raw = raw[nl + 1:] if nl >= 0 else b""
                else:
                    raw = f.read()
                self._last_stat = stat_key
        except FileNotFoundError:
            self._last_stat = None
            if not self._missing_warned:
                self._missing_warned = True
                logger.warning(
                    "外部信号文件不存在（等待通达信条件预警输出，后续同状态不再告警）：%s",
                    self._path,
                )
            return []
        except OSError:
            self._last_stat = None
            logger.warning("外部信号文件读取失败：%s", self._path, exc_info=True)
            return []
        if self._missing_warned:
            self._missing_warned = False
            logger.info("外部信号文件已出现，恢复读取：%s", self._path)
        try:
            text = raw.decode("gbk", errors="replace")
        except Exception:  # noqa: BLE001 — decode(errors=replace) 实际不抛，兜底留痕
            logger.warning("外部信号文件解码失败：%s", self._path, exc_info=True)
            return []
        parts = text.split("\n")
        parts = [p.rstrip("\r") for p in parts[:-1]]  # 末段=完整空串或半行，均不消费
        return parts

    def _warn_bad_line(self, reason: str, message_or_line: str) -> None:
        """坏行/异常状态告警去重：同一 (原因, 内容) 只告警一次（5s 轮询不刷屏）。

        去重集合有上限，超过后清空重来（避免长跑累积；清空后最多再告警一轮）。
        """
        key = (reason, message_or_line)
        if key in self._warned_lines:
            return
        if len(self._warned_lines) >= _MAX_WARNED_LINES:
            self._warned_lines.clear()
        self._warned_lines.add(key)
        if reason == "size_cap":
            logger.warning(message_or_line)
        else:
            logger.warning("外部信号行%s，跳过：%r", message_or_line, key[1])

    @staticmethod
    def _detect_layout(cols: List[str]):
        """按首列探测列布局：首列是 6 位代码 → 当前版本（无行号）；
        首列是纯数字行号且次列是 6 位代码 → 早期版本（带行号）。"""
        if len(cols) >= _LAYOUT_PLAIN[5] and _CODE_RE.match(cols[0]):
            return _LAYOUT_PLAIN
        if (len(cols) >= _LAYOUT_LEGACY[5] and cols[0].isdigit()
                and _CODE_RE.match(cols[1])):
            return _LAYOUT_LEGACY
        return None

    @staticmethod
    def _looks_like_header(cols: List[str]) -> bool:
        """表头行识别：通达信部分版本会在文件首行输出「代码 名称 …」表头。

        只做宽松匹配（避免把股票名含「代码」的极端行当表头：还要求该行时间列
        解析失败/不存在）。表头跳过不告警（避免每轮刷 WARNING）。
        """
        head = "\t".join(cols[:3])
        return ("代码" in head or "名称" in head or "预警" in head) and (
            len(cols) < 4 or not any(
                ExternalSignalFileSource._parse_time(c)
                for c in cols[2:4]))

    @staticmethod
    def _parse_time(text: str) -> Optional[datetime]:
        for fmt in _TIME_FORMATS:
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None

    def _parse_line(self, line: str):
        """单行解析：布局探测 → 代码 6 位 → 公式名匹配 → 时间/价格解析。
        任何一步失败返回 None（跳过 + warning，去重）。"""
        cols = [c.strip() for c in line.split("\t")]
        layout = self._detect_layout(cols)
        if layout is None:
            if self._looks_like_header(cols):
                return None  # 表头：静默跳过（不算坏行，不产生告警）
            self._warn_bad_line(
                "列数/代码",
                "列数不足或代码列无效（%d 列，首列 %r）" % (len(cols), cols[0]),
            )
            return None
        code_idx, name_idx, time_idx, price_idx, formula_idx, _min_cols = layout
        code = re.sub(r"\D", "", cols[code_idx])
        if len(code) != 6:
            self._warn_bad_line("代码", "代码无效（%r）" % cols[code_idx])
            return None
        formula = cols[formula_idx]
        if formula not in self._formula_names:
            return None  # 未匹配公式名：正常静默跳过（文件可能混有其它公式预警）
        ts = self._parse_time(cols[time_idx])
        if ts is None:
            self._warn_bad_line("时间", "时间解析失败（%r）" % cols[time_idx])
            return None
        try:
            price = Decimal(cols[price_idx])
        except InvalidOperation:
            self._warn_bad_line("价格", "价格解析失败（%r）" % cols[price_idx])
            return None
        return {
            "code": code,
            "name": cols[name_idx],
            "time": ts,
            "price": price,
            "formula_name": formula,
        }
