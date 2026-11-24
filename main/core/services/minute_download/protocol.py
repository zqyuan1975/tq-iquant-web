"""通达信 7709 行情主站协议 — 最小自实现（只覆盖本期需要的报文）。

设计约束（``docs/plans/0022-内置分钟数据下载-设计.md``）：
- 协议按公开的 7709 二进制格式**独立编写**，不 import 任何第三方行情客户端库，
  不复制外部工具/第三方库的代码；
- 只实现握手（0x000D 两段）、心跳（0x044E 证券数量）、K 线（0x052D，
  1 分钟的 category=8）；
- 解析逐条校验，畸形记录丢弃并计数，不整包失败。

报文布局（实测校准，2026-11）::

    响应帧 = 16B header + payload
      header : <IIIHH>（12 字节计数/回显 + zip_size + unzip_size，均 u16 LE）
      zip_size == unzip_size 时 payload 即明文；否则为 zlib 压缩体

    K 线请求 36B : <HIHHHH6sHHHHIIH>
      0x010C | 0x01016408 | 0x1C | 0x1C | 0x052D
      | market(u16) | code(6s ASCII) | category(u16) | 1(u16)
      | start(u16) | count(u16) | 0(u32) | 0(u32) | 0(u16)

    K 线响应 body : count(u16) + count 条变长记录
      每条 : zipday(u16) + tminutes(u16)
             + open/close/high/low 四个**前向差分变长整数**
             + vol(u32) + amount(u32)（4 字节压缩浮点）

    差分还原（价格单位 0.001 元）::

        open_i  = open_diff + base
        close_i = open_i + close_diff
        high_i  = open_i + high_diff
        low_i   = open_i + low_diff
        base    = close_i          # 下一条的基准

1m 的 ``zipday = (year-2004)<<11 | month*100 | day``、``tminutes =
hour*60 + minute``；``vol`` 实测单位为**股**（amount/vol ≈ 价格），落库直接沿用。

注意：§10.6 的联网探针是真实主站的唯一验收门槛；若主站帧头与上述布局有出入，
只需改 :func:`read_response_frame` 一处，解析层与存储层不受影响。
"""
from __future__ import annotations

import re
import socket
import struct
import threading
import zlib
from dataclasses import dataclass
from datetime import date as _date, time as _time
from typing import Callable, Iterable, List, Optional, Tuple

#: 1 分钟 K 线在 7709 协议中的 category（7 是扩展行情 1m，8 是标准 1m）。
CATEGORY_1MIN = 8
PERIOD_RAW_1M = CATEGORY_1MIN

#: 单页记录上限 / start 偏移上限（协议字段为 u16）。
MAX_PAGE = 800
MAX_START = 65535

#: 单帧 body 上限，防御异常主站给出超大长度。
MAX_FRAME_BYTES = 4 * 1024 * 1024

#: K 线请求固定字段：0x010C / 0x01016408 / 0x1C / 0x1C / 0x052D。
_BARS_FMT = "<HIHHHH6sHHHHIIH"
_BARS_HEAD = (0x010C, 0x01016408, 0x1C, 0x1C, 0x052D)

#: 握手：两段会话参数。实测第三段「客户端信息」会把连接切到另一种模式
#: （K 线请求只回 2 字节废帧），故不发送。
SETUP_PACKETS: Tuple[bytes, ...] = (
    bytes.fromhex("0c0218930001030003000d0001"),
    bytes.fromhex("0c0218940001030003000d0002"),
)

#: 心跳复用「证券数量」请求（0x044E），前 10 字节固定，尾部 4 字节为魔数。
_HEARTBEAT_PREFIX = bytes.fromhex("0c0c186c0001080008004e04")
_HEARTBEAT_TAIL = bytes.fromhex("75c73301")

_AM_OPEN = _time(9, 30)
_AM_CLOSE = _time(11, 30)
_PM_OPEN = _time(13, 0)
_PM_CLOSE = _time(15, 0)


class ProtocolError(Exception):
    """协议层错误（帧不完整、长度非法、解压失败等）。"""


@dataclass(frozen=True)
class BarRecord:
    """一条 1m K 线（未复权原始价；成交量为**股**，实测 amount/volume≈价格）。"""

    day: _date
    minute: _time
    open: float
    high: float
    low: float
    close: float
    amount: float
    volume: float

    @property
    def bar_time(self) -> str:
        """``YYYY-MM-DD HH:MM``（bar 结束时刻，与 TDX 1m 标签一致）。"""
        return "%s %02d:%02d" % (self.day.isoformat(), self.minute.hour,
                                 self.minute.minute)

    @property
    def stime(self) -> str:
        """14 位 bar 标签（BarPoller/native_minute 使用的 stime 口径）。"""
        return "%s%02d%02d00" % (self.day.strftime("%Y%m%d"),
                                 self.minute.hour, self.minute.minute)


def encode_setup() -> Tuple[bytes, ...]:
    """三段握手请求（依次发送，每段各读一帧响应）。"""
    return SETUP_PACKETS


def encode_heartbeat(market: int = 0) -> bytes:
    """心跳（0x044E 证券数量，market 0=深 / 1=沪）。"""
    return (_HEARTBEAT_PREFIX + struct.pack("<H", int(market) & 0xFFFF)
            + _HEARTBEAT_TAIL)


def encode_bars_request(market: int, code6: str, start: int = 0,
                        count: int = MAX_PAGE,
                        category: int = CATEGORY_1MIN) -> bytes:
    """K 线请求（0x052D）。``start`` 为自最新一根向前的偏移。"""
    code6 = str(code6 or "").strip()
    if len(code6) != 6 or not code6.isdigit():
        raise ValueError("code 必须是 6 位数字: %r" % (code6,))
    if not 0 <= int(market) <= 2:
        raise ValueError("market 非法: %r" % (market,))
    start = int(start)
    count = int(count)
    if not 0 <= start <= MAX_START:
        raise ValueError("start 越界: %d" % start)
    if not 1 <= count <= MAX_PAGE:
        raise ValueError("count 越界: %d" % count)
    return struct.pack(
        _BARS_FMT, _BARS_HEAD[0], _BARS_HEAD[1], _BARS_HEAD[2], _BARS_HEAD[3],
        _BARS_HEAD[4], int(market), code6.encode("ascii"), int(category),
        1, start, count, 0, 0, 0)


def normalize_code(code: str) -> Tuple[int, str]:
    """代码 → ``(market, 6 位代码)``。

    市场号：0=深圳、1=上海、2=北京。带后缀时以后缀为准；否则按首位推断
    （``92`` 开头的北交所新代码优先于「9→上海」规则）。
    """
    raw = str(code or "").strip().upper()
    suffix = ""
    if "." in raw:
        head, _, suffix = raw.partition(".")
        raw = head
    digits = re.sub(r"\D", "", raw)[:6]
    if len(digits) != 6:
        raise ValueError("code 无法规整为 6 位: %r" % (code,))
    if suffix in ("SH", "SS"):
        return 1, digits
    if suffix in ("SZ",):
        return 0, digits
    if suffix in ("BJ",):
        return 2, digits
    if digits.startswith("92"):
        return 2, digits
    if digits[0] in ("6", "5", "9"):
        return 1, digits
    if digits[0] in ("4", "8"):
        return 2, digits
    return 0, digits


def is_valid_bar_time(day: _date, minute: _time) -> bool:
    """bar 标签必须落在 A 股连续竞价时段（周末直接丢弃）。

    允许 09:30（部分主站首根标签为 09:30）与 11:30/13:00/15:00 边界；
    节假日不在协议层判定（fail-open，与 ``TradingCalendar`` 一致）。
    """
    if day.weekday() >= 5:
        return False
    if _AM_OPEN <= minute <= _AM_CLOSE:
        return True
    return _PM_OPEN <= minute <= _PM_CLOSE


def _decode_bar_date(raw: int) -> Optional[_date]:
    """``zipday`` → 日期：高 5 位为 year-2004，其余为 month*100+day。"""
    year = (raw >> 11) + 2004
    month = (raw % 2048) // 100
    day = (raw % 2048) % 100
    try:
        return _date(year, month, day)
    except ValueError:
        return None


def _decode_bar_time(raw: int) -> Optional[_time]:
    hour, minute = divmod(int(raw), 60)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return _time(hour, minute)


def _decode_volume(raw: int) -> float:
    """成交量/成交额的 4 字节压缩浮点解码（指数在高字节，尾数带隐含位）。"""
    raw = int(raw) & 0xFFFFFFFF
    if raw == 0:
        return 0.0
    exponent = (raw >> 24) & 0xFF
    hi = (raw >> 16) & 0xFF
    mid = (raw >> 8) & 0xFF
    lo = raw & 0xFF

    e0 = exponent * 2 - 0x7F
    e1 = exponent * 2 - 0x86
    e2 = exponent * 2 - 0x8E
    e3 = exponent * 2 - 0x96

    total = 2.0 ** e0
    if hi > 0x80:
        total += 2.0 ** e1 * 128.0 + (hi & 0x7F) * (2.0 ** (e1 + 1))
    else:
        total += 2.0 ** e1 * hi
    tail_mid = 2.0 ** e2 * mid
    tail_lo = 2.0 ** e3 * lo
    if hi & 0x80:
        tail_mid *= 2.0
        tail_lo *= 2.0
    return total + tail_mid + tail_lo


def _read_varint(body: bytes, pos: int) -> Tuple[int, int]:
    """读一个「类 utf-8」有符号变长整数，返回 ``(值, 新位置)``。

    首字节低 6 位为数值、bit6 为符号、bit7 为续位；续字节各贡献 7 位。
    """
    first = body[pos]
    pos += 1
    value = first & 0x3F
    negative = bool(first & 0x40)
    shift = 6
    if first & 0x80:
        while True:
            b = body[pos]
            pos += 1
            value += (b & 0x7F) << shift
            shift += 7
            if not b & 0x80:
                break
    return (-value if negative else value), pos


def parse_bars_body(body: bytes) -> Tuple[List[BarRecord], int]:
    """解析 0x052D 响应 body，返回 ``(按时间升序去重的记录, 丢弃条数)``。

    记录为变长（四个差分整数 + 两个 u32），逐条校验：日期/时间可解码、落在
    竞价时段、收盘价 > 0 且各价非负。重复标签以后到的为准（同一轮内主站重发）。
    """
    if not body or len(body) < 2:
        return [], 0
    total = struct.unpack_from("<H", body, 0)[0]
    pos = 2
    base = 0
    by_stime: dict = {}
    dropped = 0
    for _ in range(total):
        if pos + 12 > len(body):
            dropped += 1
            break
        try:
            raw_day, raw_time = struct.unpack_from("<HH", body, pos)
            pos += 4
            open_diff, pos = _read_varint(body, pos)
            close_diff, pos = _read_varint(body, pos)
            high_diff, pos = _read_varint(body, pos)
            low_diff, pos = _read_varint(body, pos)
            vol_raw, amt_raw = struct.unpack_from("<II", body, pos)
            pos += 8
        except (IndexError, struct.error):
            dropped += 1
            break
        open_i = base + open_diff
        close_i = open_i + close_diff
        high_i = open_i + high_diff
        low_i = open_i + low_diff
        base = close_i
        day = _decode_bar_date(raw_day)
        minute = _decode_bar_time(raw_time)
        if day is None or minute is None or not is_valid_bar_time(day, minute):
            dropped += 1
            continue
        prices = (open_i, high_i, low_i, close_i)
        if close_i <= 0 or any(p < 0 for p in prices):
            dropped += 1
            continue
        rec = BarRecord(day=day, minute=minute,
                        open=open_i / 1000.0, high=high_i / 1000.0,
                        low=low_i / 1000.0, close=close_i / 1000.0,
                        amount=_decode_volume(amt_raw),
                        volume=_decode_volume(vol_raw))
        by_stime[rec.stime] = rec
    return [by_stime[k] for k in sorted(by_stime)], dropped


def read_response_frame(recv_exact: Callable[[int], bytes]) -> bytes:
    """从连接读取一帧并返回 body（必要时自动 zlib 解压）。

    ``recv_exact(n)`` 必须返回恰好 n 字节，否则抛 :class:`ProtocolError`。
    主站帧头与本节假设不一致时，只需替换本函数。
    """
    header = recv_exact(16)
    if len(header) != 16:
        raise ProtocolError("响应头长度不足: %d" % len(header))
    # 实测布局：<IIIHH>，长度字段在 header[12:14]/[14:16]（u16 LE）。
    _seq, _echo, _cmd, zip_size, unzip_size = struct.unpack("<IIIHH", header)
    if zip_size > MAX_FRAME_BYTES or unzip_size > MAX_FRAME_BYTES:
        raise ProtocolError(
            "帧长度异常 zip=%d unzip=%d" % (zip_size, unzip_size))
    if zip_size == 0:
        return b""
    payload = recv_exact(zip_size)
    if len(payload) != zip_size:
        raise ProtocolError("body 长度不足: %d/%d" % (len(payload), zip_size))
    if zip_size == unzip_size:
        return payload
    try:
        body = zlib.decompress(payload)
    except zlib.error as exc:  # pragma: no cover - 依赖真实主站
        raise ProtocolError("解压失败: %s" % exc) from exc
    if unzip_size and len(body) != unzip_size:
        raise ProtocolError("解压长度不符: %d/%d" % (len(body), unzip_size))
    return body


def records_to_bars(records: Iterable[BarRecord]) -> List[dict]:
    """记录 → 平台 bar dict（成交量已是**股**，与 ``raw_to_bars`` 口径一致）。"""
    out = []
    for rec in records:
        out.append({
            "stime": rec.stime,
            "open": rec.open,
            "high": rec.high,
            "low": rec.low,
            "close": rec.close,
            "volume": int(round(rec.volume)),
            "amount": rec.amount,
        })
    return out


class TdxClient:
    """单主站 TCP 连接（两段握手后复用）。

    ``sock_factory`` 可注入，单测用假传输时无需真实网络。
    """

    def __init__(self, host: str, port: int = 7709, timeout: float = 5.0,
                 sock_factory=None) -> None:
        self.host = str(host)
        self.port = int(port)
        self.timeout = float(timeout)
        self._sock_factory = sock_factory or socket.create_connection
        self._sock = None
        self._lock = threading.Lock()

    def connect(self) -> "TdxClient":
        sock = self._sock_factory((self.host, self.port), timeout=self.timeout)
        try:
            sock.settimeout(self.timeout)
        except Exception:  # pragma: no cover - 假传输可能不支持
            pass
        self._sock = sock
        try:
            self.setup()
        except Exception:
            self.close()
            raise
        return self

    def close(self) -> None:
        sock = self._sock
        self._sock = None
        if sock is not None:
            try:
                sock.close()
            except Exception:  # pragma: no cover
                pass

    def _recv_exact(self, n: int) -> bytes:
        if self._sock is None:
            raise ProtocolError("连接未建立")
        chunks = []
        remaining = int(n)
        while remaining > 0:
            chunk = self._sock.recv(remaining)
            if not chunk:
                raise ProtocolError("连接被主站关闭（还需 %d 字节）" % remaining)
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _roundtrip(self, payload: bytes) -> bytes:
        with self._lock:
            if self._sock is None:
                raise ProtocolError("连接未建立")
            self._sock.sendall(payload)
            return read_response_frame(self._recv_exact)

    def setup(self) -> None:
        """握手：每段发一包、读一帧（内容不解析）。"""
        for packet in SETUP_PACKETS:
            self._roundtrip(packet)

    def heartbeat(self, market: int = 0) -> None:
        self._roundtrip(encode_heartbeat(market))

    def query_bars(self, market: int, code6: str, start: int = 0,
                   count: int = MAX_PAGE,
                   category: int = CATEGORY_1MIN) -> List[BarRecord]:
        payload = encode_bars_request(market, code6, start, count, category)
        records, _dropped = parse_bars_body(self._roundtrip(payload))
        return records
