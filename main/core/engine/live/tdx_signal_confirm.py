"""通达信 TQ 通道「交易信号」下单确认自动化（2026-11-04 真机重写）。

事实依据（2026-11-04 盘中实拍 + 客户端窗口树/OCR 离线验证）：

- 实盘账户 ``order_stock`` 恒返回 ``Value=1`` /「已发送信号至客户端，待用户确认！」。
  信号落到客户端「交易信号」窗口，**每行**两个按钮「发送」「取消」，底部
  「全部发送」「全部取消」——**没有**模态「下单确认」框（上一版按「确定 +
  仅供参考」找 #32770/自绘面板，永远匹配不到）。
- 该窗口是 CEF/Chromium 渲染（子窗口链
  ``SubWebView → CefBrowserWindow → Chrome_WidgetWin_0 →
  Chrome_RenderWidgetHostHWND``）：Win32 控件枚举只能看到 CEF host，
  UIA descendants=0，网页里的按钮**没有控件句柄**，BM_CLICK/UIA 无从下手。
- 因此可靠路径只有「窗口级截图 → OCR 定位『发送』→ 真实鼠标点击」。
  真机截图离线验证：对窗口做 4x 放大 + 反色 + 阈值二值化后，Windows OCR
  能稳定读出 002056/600888、300/900、卖出、发送、取消 与各自坐标
  （原始深色主题直接 OCR 会把按钮读成乱码）。

自动化策略（按可靠性排序）：

1. ``EnumWindows`` + 映像名 ``tdxw.exe`` + 标题含「交易信号」+ 含 CEF 子窗口
   —— 不依赖窗口位置/DPI；多实例（客户端会残留隐藏旧窗）取 **可见且最顶层**。
2. ``PrintWindow(PW_RENDERFULLCONTENT)`` 截窗口自身内容（被遮挡也能截），
   失败退回 BitBlt；截图失败才退回屏幕抓取。
3. OCR 行匹配：代码（OCR 混淆归一 S→5/O→0）+ 数量 + 方向 + 时间（±45s，
   防同代码同量的历史残留待发送行被误点——误点会真的报单）。
   整行 OCR 把价格读丢时（真机 2026-11-18：64.83 只剩「·83」），按列锚点裁
   价格列 ROI 单独 OCR（限字符集 + Otsu 自适应二值化 + 多帧投票），采纳价还
   须落在当日涨跌停区间内且与委托价偏离 ≤2%；推不出列区间则回退整行路径。
4. 点击：若该点真有 ``Button`` 型 HWND → BM_CLICK；否则先 ``SetWindowPos``
   把目标窗口置顶并用 ``WindowFromPoint`` 复核（不成立重试，仍失败则 fail-
   closed 放弃），再用真实鼠标点击（CEF 唯一可靠方式）；点击完成后还原为
   ``HWND_NOTOPMOST``。点击后复查该行是否消失/转「已发送」，再由委托表回查
   二次确认（2026-11-18 加固）。
5. 单次调用**最多点两次**（首次 + 复核仍读作『发送』时补点一次；风控单宁可
   判失败等下一根 bar 重发，也不冒重复报单风险）；失败落盘截图 + 窗口树 +
   OCR 文本到 ``logs/tdx_confirm/``。
6. 全程受 deadline 约束，不会阻塞引擎主循环无限等待。
"""
from __future__ import annotations

import ctypes
import ctypes.wintypes as wt
import logging
import os
import re
import tempfile
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

#: OCR 放大倍数（4x 可读小字按钮；2x 会把「发送」读成乱码）
_OCR_SCALE = 4
#: 反色后的二值化阈值（真机截图校准）
_OCR_THRESHOLD = 140
#: 价格列 ROI 读不出时的换档重抓参数（放大倍率 + 二值化阈值各换一档）
_OCR_SCALE_ALT = 3
_OCR_THRESHOLD_ALT = 115
#: 价格列 ROI 单独识别的参数（只裁价格列小图、限定字符集、按块自适应阈值）
_PRICE_ROI_SCALE = 6
_PRICE_ROI_MARGIN_Y = 3.0
#: 价格列多帧投票帧数与帧间隔（沿用既有轮询节奏，整体只多 ~0.3s）
_PRICE_VOTE_FRAMES = 3
_PRICE_VOTE_INTERVAL = 0.15
#: 每次 confirm 最多做几轮价格列 ROI 投票（避免每轮循环都重抓拉长等待）
_PRICE_ROI_MAX_TRIES = 2
#: 价格块允许的字符集（数字/小数点/负号；其余字符一律丢弃）
_PRICE_BLOCK_CHARS = frozenset("0123456789.．·-")
_PRICE_BLOCK_RE = re.compile(r"^(-?)(\d{1,4})[.．·](\d{1,3})$")
#: 识别价与本次委托价的允许偏离（比例，2%）
_PRICE_DEVIATION_MAX = 0.02
#: 价格碎片与数量列的最大间距（按字符宽自适应，下限 12px）
_PRICE_FRAG_GAP = 12.0
#: 目标窗口置顶重试次数与间隔（2026-11-18 实证：平台 Edge 窗口最大化压住
#: 「发送」按钮像素，单次 SetForegroundWindow 拿不到前台）
_TOPMOST_ATTEMPTS = 3
_TOPMOST_RETRY_DELAY = 0.2
#: 画面就绪等待（2026-11-18）：窗口刚被强制显示时 CEF 仍在重绘，固定 sleep
#: 0.2s 后点击会「点空」（真机：价格列整体缺失、点击未生效）。等到「行非空 +
#: 价格列区间可推 + 连续两帧行指纹一致」再点，上限 _READY_TIMEOUT。
_READY_TIMEOUT = 4.0
_READY_INTERVAL = 0.3
#: 窗口被 _force_show 强制显示后多久内仍视为「刚显示」（snapshot_rows 与
#: confirm 共用同一 confirmer 实例，时间戳跨调用有效）
_READY_GRACE = 10.0
#: 点击后复核仍为「发送」时的补点次数上限（绝不多点，防重复报单）
_CLICK_RETRY_MAX = 1
#: 同一行的 y 向容差（窗口坐标，px）
_ROW_TOL = 10.0
#: 订单发出时间与信号行时间的允许偏差（秒）。引擎按 bar 节奏下单（~60s/次），
#: 取 45s 以排除上一根 bar 的残留同形状行，又容忍客户端时钟/OCR 抖动。
_TIME_TOL_SECONDS = 45

WM_MOUSEWHEEL = 0x020A
BM_CLICK = 0x00F5
MOUSEEVENTF_LEFTDOWN = 0x0002
MOUSEEVENTF_LEFTUP = 0x0004
MOUSEEVENTF_WHEEL = 0x0800
GA_ROOT = 2

# ---- SetWindowPos 置顶相关（点击前把目标窗口提到最前，点击后还原）----
HWND_TOPMOST = -1
HWND_NOTOPMOST = -2
SWP_NOSIZE = 0x0001
SWP_NOMOVE = 0x0002
SWP_NOACTIVATE = 0x0010
SWP_SHOWWINDOW = 0x0040
GWL_EXSTYLE = -20
WS_EX_TOPMOST = 0x00000008

#: OCR 常见数字混淆（仅用于纯数字/近数字文本）
_DIGIT_CONFUSION = str.maketrans({
    "O": "0", "o": "0", "Q": "0", "D": "0",
    "S": "5", "s": "5", "I": "1", "l": "1", "|": "1",
    "B": "8", "Z": "2", "G": "6", "T": "7",
})
_TIME_RE = re.compile(r"([0-2]?\d)[:：](\d{2})[:：](\d{2})")
#: 价格列（1~3 位小数；小数点可能是「.」「．」或 OCR 出的「·」；两侧不得紧邻
#: 数字，避免把「17.64300」误读为 17.643）
_PRICE_RE = re.compile(r"(?<!\d)(\d{1,4})\s*[.．·]\s*(\d{1,3})(?!\d)")
#: 价格匹配容差（1 分钱）。限价对限价；读不出行价格一律不放行（宁失败不错）。
_PRICE_TOL = 0.011
#: 行时间下限相对本次下单时间的容差（秒）：基线不可用时的退化规则，
#: 只接受「下单之后」出现的行，绝不点更早的历史残留行。
_TIME_FLOOR_TOL_SECONDS = 2

_user32 = ctypes.WinDLL("user32", use_last_error=True)
_gdi32 = ctypes.WinDLL("gdi32", use_last_error=True)

#: DPI_AWARENESS_CONTEXT_PER_MONITOR_AWARE_V2（Win10 1607+）
_DPI_PER_MONITOR_V2 = -4


def _dpi_ctx():
    """线程级 DPI 感知上下文（不改进程，避免影响同进程 THS 自动化）。

    Core 进程默认 DPI-unaware：在非 100% 缩放下 ``GetWindowRect`` 返回 96DPI
    「逻辑坐标」，而 ``SetCursorPos`` / ``WindowFromPoint`` 用物理像素——两者
    混用会整体偏移，点不中按钮。进入本上下文后本线程所有窗口/鼠标 API 统一为
    物理像素。系统不支持（Win7/8）或设置失败 → 空上下文，按原感知工作。
    """

    class _Null:
        def __enter__(self):
            return None

        def __exit__(self, *_a):
            return False

    try:
        setter = _user32.SetThreadDpiAwarenessContext
    except AttributeError:
        return _Null()
    setter.restype = ctypes.c_void_p
    setter.argtypes = [ctypes.c_void_p]
    try:
        prev = setter(ctypes.c_void_p(_DPI_PER_MONITOR_V2))
    except Exception:  # noqa: BLE001
        return _Null()
    if not prev:
        return _Null()

    class _Ctx:
        def __enter__(self):
            return None

        def __exit__(self, *_a):
            try:
                setter(ctypes.c_void_p(prev))
            except Exception:  # noqa: BLE001
                pass
            return False

    return _Ctx()


class _BITMAPINFOHEADER(ctypes.Structure):
    _fields_ = [
        ("biSize", wt.DWORD), ("biWidth", wt.LONG), ("biHeight", wt.LONG),
        ("biPlanes", wt.WORD), ("biBitCount", wt.WORD),
        ("biCompression", wt.DWORD), ("biSizeImage", wt.DWORD),
        ("biXPelsPerMeter", wt.LONG), ("biYPelsPerMeter", wt.LONG),
        ("biClrUsed", wt.DWORD), ("biClrImportant", wt.DWORD),
    ]


# ---------------------------------------------------------------- 数据结构


@dataclass(frozen=True)
class SignalSpec:
    """本次下单在客户端「交易信号」窗口里的识别特征。"""

    code6: str
    direction: str          # buy / sell
    volume: int
    price: float = 0.0
    sent_at: Optional[datetime] = None
    #: 当日涨跌停（可选；调用方已有 /price_limit 数据时传入，缺省 0=未知）
    limit_up: float = 0.0
    limit_down: float = 0.0

    @property
    def seconds(self) -> Optional[int]:
        if self.sent_at is None:
            return None
        return (self.sent_at.hour * 3600 + self.sent_at.minute * 60
                + self.sent_at.second)


@dataclass
class WindowRef:
    hwnd: int
    title: str
    rect: Tuple[int, int, int, int]   # left, top, right, bottom
    visible: bool
    has_cef: bool = False

    @property
    def width(self) -> int:
        return max(self.rect[2] - self.rect[0], 0)

    @property
    def height(self) -> int:
        return max(self.rect[3] - self.rect[1], 0)


@dataclass
class Cell:
    text: str
    x: float
    y: float
    w: float
    h: float

    @property
    def cx(self) -> float:
        return self.x + self.w / 2

    @property
    def cy(self) -> float:
        return self.y + self.h / 2

    @property
    def right(self) -> float:
        return self.x + self.w


@dataclass
class Row:
    cells: List[Cell] = field(default_factory=list)
    #: 价格列 ROI 多帧投票得到的价格（整行 OCR 读不出时由确认循环回填；
    #: 非空即以此为准，避免整行混排识别丢字）
    price_override: Optional[float] = None

    @property
    def cy(self) -> float:
        if not self.cells:
            return 0.0
        return sum(c.cy for c in self.cells) / len(self.cells)

    @property
    def code(self) -> str:
        for c in self.cells:
            if ":" in c.text or "：" in c.text:
                continue
            n = _norm_digits(c.text)
            if len(n) == 6 and n.isdigit():
                return n
        # 代码列被 OCR 拆成多格（真机 2026-11-10 实证：001311 被拆成
        # 「001 | 31 | 1」，单格都不到 6 位）→ 原先恒返回 ""，_match_row 判
        # 「无本单行」，自动确认放弃，委托行滞留在「交易信号」窗口未发送。
        return self._reassembled_code()

    def _reassembled_code(self) -> str:
        """按「名称列左侧的连续数字片段」重组代码（001|31|1 → 001311）。

        以名称列（首个含汉字的单元格）为右锚，向左逐格累积纯数字片段，遇到
        时间列（含冒号）或非数字格即停；累计满 6 位取末 6 位——左侧编号/
        账号列（如 28500208）的数字会成为前缀，代码是紧邻名称的那一段。
        无名称列/片段不足 6 位 → ""（不猜）。
        """
        ordered = sorted(self.cells, key=lambda c: c.x)
        name_idx = None
        for i, c in enumerate(ordered):
            t = c.text.replace(" ", "")
            if any("\u4e00" <= ch <= "\u9fff" for ch in t):
                name_idx = i
                break
        if name_idx is None:
            return ""
        digits = ""
        for c in reversed(ordered[:name_idx]):
            t = c.text.strip()
            if ":" in t or "：" in t:
                break
            n = _norm_digits(t)
            if not n.isdigit() or not n:
                break
            if len(n) > 6:
                # 编号/账号列（如 28500208）不是代码碎片：遇到即停，绝不把
                # 它的末 6 位当代码返回（否则无代码行会被误判成另一只票）。
                break
            digits = n + digits
            if len(digits) >= 6:
                break
        if len(digits) < 6:
            return ""
        return digits[-6:]

    @property
    def volume(self) -> str:
        """数量列：行内最右侧的数字词（时间/账号/代码之后的数字）。"""
        out = ""
        for c in sorted(self.cells, key=lambda x: x.x):
            if ":" in c.text or "：" in c.text:
                continue
            n = _norm_digits(c.text)
            if n.isdigit() and 1 <= len(n) <= 6:
                out = n
        return out

    @property
    def direction(self) -> str:
        for c in self.cells:
            if "卖" in c.text:
                return "sell"
            if "买" in c.text:
                return "buy"
        return ""

    @property
    def seconds(self) -> Optional[int]:
        """行内时间（OCR 常把 14:43:09 拆成 5 个词，按 x 拼接后取正则）。"""
        text = "".join(c.text for c in sorted(self.cells, key=lambda x: x.x))
        m = _TIME_RE.search(text)
        if not m:
            return None
        return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))

    @property
    def price(self) -> Optional[float]:
        """行内委托价（限价）。读不出/市价行返回 None（调用方按「不可判读」处理）。

        ``price_override``（价格列 ROI 多帧投票值）优先；否则走整行 OCR 拼接。
        """
        if self.price_override is not None:
            return self.price_override
        return self.raw_price

    @property
    def raw_price(self) -> Optional[float]:
        """整行 OCR 直接拼出的价格（不受 ROI 投票回填影响，行指纹用）。

        OCR 常把 17.64 拆成「1」「7」「·」「64」多个词，必须按 x 邻近拼接后
        再取正则（``_row_text`` 只在列间距大时才插空格）；间距大导致拼不上时
        再按「小数点分隔符左右邻格」重组（真机 2026-11-10 实证 14.95 被拆成
        「1 | 4 | · | 95」）。
        """
        text = _row_text(self)
        m = _PRICE_RE.search(text)
        by_sep = self._price_by_separator()
        if by_sep is not None:
            # 小数点在独立格时以「分隔符左右邻格」为准：间距大（>8px）时
            # _row_text 会插空格，正则只能读到「4·95」→ 4.95（丢整数位）。
            return by_sep
        if m:
            return float("%s.%s" % (m.group(1), m.group(2)))
        return None

    def _price_by_separator(self) -> Optional[float]:
        """按小数点分隔符的左右邻格重组价格（``1 | 4 | · | 95`` → 14.95）。

        只吃紧邻的短线数字格（左 ≤3 位、右 ≤2 位/格，且与分隔符间距 ≤40px），
        遇到非数字或长数字格（代码 001311 / 数量 3600 / 编号 28500208）立即停，
        避免把数量拼进价格。
        """
        ws = sorted(self.cells, key=lambda c: c.x)
        for i, c in enumerate(ws):
            if c.text.strip() not in (".", "．", "·", "•"):
                continue
            left, j, edge = "", i - 1, c.x
            while j >= 0 and len(left) < 4:
                t = ws[j].text.strip()
                if not t.isdigit() or len(t) > 3 or edge - ws[j].right > 40:
                    break
                left = t + left
                edge = ws[j].x
                j -= 1
            right, j, edge = "", i + 1, c.right
            while j < len(ws) and len(right) < 4:
                t = ws[j].text.strip()
                if not t.isdigit() or len(t) > 2 or ws[j].x - edge > 40:
                    break
                right = right + t
                edge = ws[j].right
                j += 1
            if left and right:
                try:
                    return float("%s.%s" % (left, right))
                except ValueError:
                    continue
        return None

    @property
    def sent(self) -> bool:
        """行尾按钮是否已变为「已发送」（点击成功后客户端会改按钮文案）。"""
        return any("已发送" in c.text.replace(" ", "") for c in self.cells)

    @property
    def send(self) -> Optional[Cell]:
        """本行「发送」按钮单元格（「发」「送」常被 OCR 拆成两个词）。"""
        ws = sorted(self.cells, key=lambda c: c.x)
        for i, w in enumerate(ws):
            t = w.text.replace(" ", "")
            if "发送" in t:
                return w
            if t == "发" and i + 1 < len(ws):
                nxt = ws[i + 1]
                nt = nxt.text.replace(" ", "")
                if nt.startswith("送") and nxt.x - w.right <= 4 and \
                        abs(nxt.cy - w.cy) <= 3:
                    return Cell(
                        text="发送", x=w.x, y=min(w.y, nxt.y),
                        w=max(nxt.right, w.right) - w.x,
                        h=max(w.h, nxt.h),
                    )
        return None


@dataclass
class ConfirmOutcome:
    ok: bool
    detail: str
    clicks: int = 0
    method: str = ""
    stale: int = 0


class _WindowEnumProc:
    """EnumWindows 回调保持引用（防 GC）。"""

    def __init__(self):
        self.rows: List[Tuple[int, str, int, bool]] = []
        self._proc = ctypes.WINFUNCTYPE(wt.BOOL, wt.HWND, wt.LPARAM)(self._cb)

    def _cb(self, hwnd, _lparam):
        pid = wt.DWORD()
        _user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
        image = ""
        try:
            from .win_dialog_scope import window_image_name
            image = window_image_name(pid.value)
        except Exception:  # noqa: BLE001
            image = ""
        if image != TdxSignalConfirmer.PROCESS_IMAGE:
            return True
        self.rows.append((
            int(hwnd), _window_text(int(hwnd)), int(pid.value),
            bool(_user32.IsWindowVisible(hwnd)),
        ))
        return True

    def run(self) -> List[Tuple[int, str, int, bool]]:
        _user32.EnumWindows(self._proc, 0)
        return self.rows


# ---------------------------------------------------------------- 纯逻辑辅助


def _norm_digits(text: str) -> str:
    """OCR 数字混淆归一：0020S6 → 002056；仅保留数字。"""
    s = (text or "").translate(_DIGIT_CONFUSION)
    return re.sub(r"\D", "", s)


def _seconds_diff(a: int, b: int) -> int:
    d = abs(a - b)
    return min(d, 86400 - d)


def _window_text(hwnd: int) -> str:
    buf = ctypes.create_unicode_buffer(512)
    _user32.GetWindowTextW(wt.HWND(hwnd), buf, 512)
    return buf.value


def _window_class(hwnd: int) -> str:
    buf = ctypes.create_unicode_buffer(256)
    _user32.GetClassNameW(wt.HWND(hwnd), buf, 256)
    return buf.value


def _window_rect(hwnd: int) -> Tuple[int, int, int, int]:
    r = wt.RECT()
    _user32.GetWindowRect(wt.HWND(hwnd), ctypes.byref(r))
    return (int(r.left), int(r.top), int(r.right), int(r.bottom))


def _children(hwnd: int) -> List[int]:
    out: List[int] = []
    child = _user32.GetWindow(wt.HWND(hwnd), 5)   # GW_CHILD
    while child:
        out.append(int(child))
        child = _user32.GetWindow(wt.HWND(child), 2)  # GW_HWNDNEXT
    return out


def _walk(hwnd: int, depth: int, max_depth: int, out: List[tuple]) -> None:
    if depth > max_depth:
        return
    rect = _window_rect(hwnd)
    out.append((
        depth, hwnd, _window_class(hwnd), _window_text(hwnd), rect,
        bool(_user32.IsWindowVisible(wt.HWND(hwnd)), ),
        _user32.GetDlgCtrlID(wt.HWND(hwnd)),
    ))
    for c in _children(hwnd):
        _walk(c, depth + 1, max_depth, out)


def _has_cef_child(hwnd: int) -> bool:
    """窗口内是否含 CEF 渲染窗口（判断「这是网页版交易界面」）。"""
    targets = ("Chrome_RenderWidgetHostHWND", "CefBrowserWindow",
               "Chrome_WidgetWin_0", "Chrome_WidgetWin_1")
    stack = [(hwnd, 0)]
    while stack:
        cur, depth = stack.pop()
        if depth > 6:
            continue
        for c in _children(cur):
            if _window_class(c) in targets:
                return True
            stack.append((c, depth + 1))
    return False


def _front_root_at(sx: float, sy: float) -> int:
    """点 (sx,sy) 处最上层窗口的顶层句柄（0 = 该点无窗口）。"""
    try:
        hwnd = _user32.WindowFromPoint(wt.POINT(int(sx), int(sy)))
        return int(_user32.GetAncestor(hwnd, GA_ROOT) or 0)
    except Exception:  # noqa: BLE001
        return 0


def _is_topmost(hwnd: int) -> bool:
    """窗口是否已是置顶（WS_EX_TOPMOST）。读失败按「非置顶」处理。"""
    try:
        ex = int(_user32.GetWindowLongW(wt.HWND(hwnd), GWL_EXSTYLE) or 0)
        return bool(ex & WS_EX_TOPMOST)
    except Exception:  # noqa: BLE001
        return False


def _set_topmost(hwnd: int, topmost: bool) -> bool:
    """SetWindowPos 置顶/取消置顶（不动尺寸/位置/焦点）。

    后台进程也能用（不像 SetForegroundWindow 受前台权限限制）；点击完成后
    调用方必须用 ``topmost=False`` 还原，避免通达信长期悬浮在其它窗口之上。
    """
    try:
        flag = HWND_TOPMOST if topmost else HWND_NOTOPMOST
        insert_after = ctypes.c_void_p(flag & 0xFFFFFFFFFFFFFFFF)
        cmd = SWP_NOSIZE | SWP_NOMOVE | SWP_NOACTIVATE
        if topmost:
            cmd |= SWP_SHOWWINDOW
        return bool(_user32.SetWindowPos(
            wt.HWND(hwnd), insert_after, 0, 0, 0, 0, cmd))
    except Exception:  # noqa: BLE001
        logger.exception("tdx 交易信号窗口 SetWindowPos 失败（topmost=%s）", topmost)
        return False


def _pick_window_impl(items: Sequence[WindowRef]) -> Optional[WindowRef]:
    """从候选中挑本次要操作的「交易信号」窗口。

    优先 可见+CEF，其次 可见，再次 CEF；同档取 **枚举顺序最靠前**（EnumWindows
    自上而下 = 最顶层）。全部隐藏也返回唯一 CEF 候选（调用方可强制显示）。
    """
    if not items:
        return None
    for pred in (
        lambda w: w.visible and w.has_cef,
        lambda w: w.visible,
        lambda w: w.has_cef,
    ):
        for w in items:
            if pred(w):
                return w
    return None


def _build_rows(words: Iterable[tuple]) -> List[Row]:
    """把 OCR 词（text, x, y, w, h；窗口坐标）按 y 聚成行。

    单元格不在这里合并——列与列之间可能只差几个像素（数量「300」与按钮
    「发」实测会重叠），贪心合并会把不同列粘成「300发送」。时间/按钮等需要
    合并的语义改由 ``Row.seconds`` / ``Row.send`` 按 x 邻近关系精确处理。
    """
    items = [Cell(str(t), float(x), float(y), float(w), float(h))
             for (t, x, y, w, h) in words if t]
    items.sort(key=lambda c: c.cy)
    rows: List[Row] = []
    for cell in items:
        for row in rows:
            if abs(row.cy - cell.cy) <= _ROW_TOL:
                row.cells.append(cell)
                break
        else:
            rows.append(Row(cells=[cell]))
    for row in rows:
        row.cells.sort(key=lambda c: c.x)
    return rows


def _row_text(row: Row, gap_px: float = 8.0) -> str:
    """按 x 顺序拼接行内文本。

    同一语义词常被 OCR 拆成多个碎片（如价格「1」「7」「·」「64」），相邻
    单元格间距 ≤ gap_px 时直接相连；列间距（数量/按钮）更大，插空格分隔，
    避免「17.64300」这类跨列粘连。
    """
    out: List[str] = []
    prev: Optional[Cell] = None
    for c in sorted(row.cells, key=lambda x: x.x):
        if prev is not None and c.x - prev.right > gap_px:
            out.append(" ")
        out.append(c.text.replace(" ", ""))
        prev = c
    return "".join(out)


def _row_fingerprint(row: Row) -> tuple:
    """行指纹（基线扣除用）：代码/方向/数量/价格/行时间 全同视为同一旧行。

    时间精确到秒、价格精确到分；基线在 order_stock 之前拍，新行时间必然
    不同，因此不会把本次新行误当旧行排除。价格取整行 OCR 原值（``raw_price``），
    不受价格列 ROI 投票回填影响——否则点击后复核会因「回填价 vs 未回填价」
    指纹不同误判成「行已消失」。
    """
    price = row.raw_price
    return (row.code, row.direction, row.volume,
            round(price, 2) if price is not None else None, row.seconds)


def _subtract_baseline(rows: Sequence[Row],
                       baseline: Sequence[tuple]) -> List[Row]:
    """扣除基线中已存在的旧行（多重集扣除，同指纹多行逐条抵消）。"""
    if not baseline:
        return list(rows)
    left = Counter(baseline)
    kept: List[Row] = []
    for row in rows:
        fp = _row_fingerprint(row)
        if left.get(fp, 0) > 0:
            left[fp] -= 1
            continue
        kept.append(row)
    return kept


def _match_row(rows: Sequence[Row], spec: SignalSpec,
               tol_seconds: int = _TIME_TOL_SECONDS,
               baseline: Optional[Sequence[tuple]] = None,
               allow_price_missing: bool = True) -> Optional[Row]:
    """定位「本次委托」对应的待发送行（找不到 → None，宁可不点也不误点）。

    硬条件：代码 6 位一致 + 数量一致 + 方向一致（读不出方向则放行）+
    **价格一致**（限价对限价，±1 分钱）。
    价格兜底（2026-11-18 实证：OCR 把 64.83 读成「·83」整行丢价格）：价格列
    读不出时，若「代码+方向+数量+行时间」在当帧窗口里**唯一**命中一行则认这
    一行；多行候选一律放弃（不许猜）。价格列 ROI 已给出价但与委托价不符时
    调用方传 ``allow_price_missing=False``，此时不再走该兜底（冲突证据不放行）。
    基线条件：``baseline``（下单前已存在的行指纹）里的行全部排除——只点
    基线之后新出现的行，哪怕旧行时间更接近本次下单。
    时间条件：行时间不得早于本次下单前 ``_TIME_FLOOR_TOL_SECONDS`` 秒，且
    与下单时间差 ≤ tol_seconds；**没有任何可读时间**时，只有唯一候选才放行。
    多行命中取时间最接近、其次最下方（最新）。
    """
    if not spec.code6:
        return None
    vol = str(int(spec.volume))
    want_price = float(spec.price or 0.0)
    floor = (spec.seconds - _TIME_FLOOR_TOL_SECONDS
             if spec.seconds is not None else None)
    cands: List[Tuple[float, float, Row]] = []
    # 价格列 OCR 读不出的行：只在全窗口唯一命中时降级放行（见函数末尾）
    no_price: List[Tuple[float, float, Row]] = []
    # 代码列 OCR 完全不可读的行（重组后仍为空）：只作为「唯一命中」的降级候选，
    # 需同时满足 数量+方向+价格+时间 全一致，且窗口内唯一（见下）。
    loose: List[Tuple[float, float, Row]] = []
    for row in _subtract_baseline(rows, baseline or ()):
        if row.code != spec.code6:
            if row.code or not spec.seconds:
                continue
            if row.volume != vol or row.send is None or want_price <= 0:
                continue
            if row.direction and row.direction != spec.direction:
                continue
            row_price = row.price
            t = row.seconds
            if (row_price is None or abs(row_price - want_price) > _PRICE_TOL
                    or t is None
                    or _seconds_diff(t, spec.seconds) > tol_seconds):
                continue
            loose.append((_seconds_diff(t, spec.seconds), row.cy, row))
            continue
        if row.volume != vol:
            continue
        if row.direction and row.direction != spec.direction:
            continue
        if row.send is None:
            continue
        t = row.seconds
        if t is None:
            if spec.seconds is not None:
                continue
        else:
            if floor is not None and t < floor:
                continue
            if spec.seconds is not None and \
                    _seconds_diff(t, spec.seconds) > tol_seconds:
                continue
        delta = (_seconds_diff(t, spec.seconds)
                 if (t is not None and spec.seconds is not None) else 0)
        if want_price > 0:
            row_price = row.price
            if row_price is None:
                no_price.append((delta, row.cy, row))
                continue
            if abs(row_price - want_price) > _PRICE_TOL:
                continue
        cands.append((delta, row.cy, row))
    if cands:
        if spec.seconds is None:
            # 无时间基准：仅唯一候选放行（历史残留行可能真的报单）
            return cands[0][2] if len(cands) == 1 else None
        return min(cands, key=lambda c: (c[0], -c[1]))[2]
    # 价格列读不出但「代码+方向+数量+时间」唯一命中 → 认这一行（重抓一档后
    # 仍唯一才到这里；多候选一律放弃，绝不对坐标做猜测偏移）。
    if not loose and len(no_price) == 1:
        if not allow_price_missing:
            logger.warning(
                "tdx 交易信号：价格列 ROI 给出的价格无法确认本行，"
                "放弃「唯一命中」降级放行（宁漏不误）")
            return None
        logger.warning(
            "tdx 交易信号：本行价格列 OCR 缺失，按「代码+方向+数量+行时间」"
            "唯一命中放行（行时间 %s 数量 %s）",
            _fmt_row_seconds(no_price[0][2].seconds), vol)
        return no_price[0][2]
    if len(loose) == 1 and not no_price:
        # 降级：代码列读不出但时间+价格+数量唯一命中（宁可不点也不误点）——
        # 多候选时不放行，等下一轮渲染。
        logger.warning(
            "tdx 交易信号：本次行代码列 OCR 不可读，按「时间+价格+数量」"
            "唯一命中降级匹配（行时间 %s 价格 %s 数量 %s）",
            _fmt_row_seconds(loose[0][2].seconds), loose[0][2].price, vol)
        return loose[0][2]
    return None


def _count_price_unreadable_rows(rows: Sequence[Row], spec: SignalSpec,
                                 baseline: Optional[Sequence[tuple]] = None,
                                 tol_seconds: int = _TIME_TOL_SECONDS) -> int:
    """失败文案区分用：同代码/方向/数量/时间窗、但价格列 OCR 读不出的行数。

    >0 说明匹配失败的原因是「价格读不出（或候选不唯一）」，而不是「可视区
    未见本次委托行」——两类原因必须分开提示，否则排查会被带偏。
    """
    return len(_price_unreadable_rows(rows, spec, baseline, tol_seconds))


def _price_unreadable_rows(rows: Sequence[Row], spec: SignalSpec,
                           baseline: Optional[Sequence[tuple]] = None,
                           tol_seconds: int = _TIME_TOL_SECONDS) -> List[Row]:
    """「代码+方向+数量+时间」都对得上、只缺价格的行（ROI 价格回填的候选）。"""
    vol = str(int(spec.volume))
    out: List[Row] = []
    for row in _subtract_baseline(rows, baseline or ()):
        if row.code != spec.code6 or row.volume != vol or row.send is None:
            continue
        if row.direction and row.direction != spec.direction:
            continue
        if row.price is not None:
            continue
        t = row.seconds
        if spec.seconds is not None:
            if t is None or _seconds_diff(t, spec.seconds) > tol_seconds:
                continue
        out.append(row)
    return out


def _pending_unsent_row(rows: Sequence[Row], spec: SignalSpec,
                        tol_seconds: int = _TIME_TOL_SECONDS) -> Optional[bool]:
    """本单行是否仍停在「待发送」（点击是否真的生效的判定口径）。

    True=唯一候选且行尾仍读作『发送』（未点出去）；False=候选不存在或已转
    『已发送』；None=多候选/无法唯一判定（不猜，调用方按 unknown 处理）。
    只看「代码（可读时）+方向+数量+行时间」：待发送帧的价格列常整体缺失
    （2026-11-18 实证），带价格条件会把「仍在待发送」误判成「行不存在」。
    """
    vol = str(int(spec.volume))
    cands: List[Row] = []
    for row in rows:
        if row.code and row.code != spec.code6:
            continue
        if row.volume != vol:
            continue
        if row.direction and row.direction != spec.direction:
            continue
        t = row.seconds
        if spec.seconds is not None:
            if t is None or _seconds_diff(t, spec.seconds) > tol_seconds:
                continue
        cands.append(row)
    if not cands:
        return False
    if len(cands) > 1:
        return None
    row = cands[0]
    # 「已发送」文本包含子串「发送」，必须先判 sent 再判 send
    if row.sent:
        return False
    if row.send is not None:
        return True
    return None


def _parse_price_block(text: str) -> Optional[float]:
    """价格块文本 → 价格（限定字符集：数字/小数点/负号，其余一律丢弃）。

    丢弃后必须恰好是「[-]整数(1~4位).小数(1~3位)」；拼接后出现多个小数点、
    缺整数位（如只读到「·83」）、混入数量等多段数字，一律判不可用。
    """
    s = "".join(ch for ch in (text or "") if ch in _PRICE_BLOCK_CHARS and
                not ch.isspace())
    m = _PRICE_BLOCK_RE.match(s)
    if not m:
        return None
    try:
        val = float("%s%s.%s" % (m.group(1), m.group(2), m.group(3)))
    except ValueError:
        return None
    return val if val > 0 else None


def _otsu_threshold(gray) -> Optional[int]:
    """按块自算 Otsu 二值化阈值（覆盖深色反色后各列亮度不一致）。

    图像近乎单峰/全黑全白时返回 None，由调用方回退到固定阈值。
    """
    try:
        hist = gray.histogram()
    except Exception:  # noqa: BLE001
        return None
    total = sum(hist)
    if total <= 0:
        return None
    used = [i for i, n in enumerate(hist) if n]
    if len(used) < 2 or (used[-1] - used[0]) < 20:
        return None
    sum_all = sum(i * n for i, n in enumerate(hist))
    sum_b = 0.0
    w_b = 0
    best, best_t = -1.0, None
    for t in range(256):
        w_b += hist[t]
        if w_b == 0:
            continue
        w_f = total - w_b
        if w_f <= 0:
            break
        sum_b += t * hist[t]
        m_b = sum_b / w_b
        m_f = (sum_all - sum_b) / w_f
        var = float(w_b) * w_f * (m_b - m_f) ** 2
        if var > best:
            best, best_t = var, t
    return best_t


def _price_column_band(rows: Sequence[Row]) -> Optional[Tuple[float, float]]:
    """从当帧行锚点推算价格列 x 区间（含容错边距）；推不出 → None。

    锚点：每行「发送」左侧的数量列（右锚）+ 紧邻数量列左侧、只含价格字符
    的连续碎片（左锚，OCR 丢字时可能只剩「·83」）。左边界再向左留 3 个
    字符宽（覆盖整行识别时被丢掉的「64」）；右侧止于数量列左沿，避免把
    数量拼进价格。任一帧推不出（如价格全部读不出）→ None，调用方回退到
    整行识别路径，不得因此直接判失败。
    """
    lefts: List[float] = []
    rights: List[float] = []
    char_ws: List[float] = []
    for row in rows:
        if row.send is None:
            continue
        before = [c for c in row.cells if c.x < row.send.x - 2]
        vol = None
        for c in sorted(before, key=lambda c: c.x, reverse=True):
            t = c.text.strip()
            if not t or ":" in t or "：" in t:
                continue
            n = _norm_digits(t)
            if n.isdigit() and 1 <= len(n) <= 6:
                vol = c
                break
        if vol is None:
            continue
        gap_limit = max(_PRICE_FRAG_GAP,
                        2.5 * (vol.w / max(len(vol.text.strip()), 1)))
        frags: List[Cell] = []
        edge = vol.x
        for c in sorted(before, key=lambda c: c.x, reverse=True):
            if c.x >= vol.x:
                continue
            t = c.text.strip()
            if edge - c.right > gap_limit:
                break
            if not t or ":" in t or "：" in t:
                break
            if not all(ch in _PRICE_BLOCK_CHARS for ch in t):
                break
            frags.append(c)
            edge = c.x
        if not frags:
            continue
        lefts.append(min(c.x for c in frags))
        rights.append(vol.x - 1.0)
        char_ws.extend(c.w / max(len(c.text.strip()), 1) for c in frags)
    if not lefts:
        return None
    char_w = sorted(char_ws)[len(char_ws) // 2] if char_ws else 7.0
    x0 = min(lefts) - 3.0 * char_w
    x1 = max(rights)
    if x1 - x0 < 8.0:
        return None
    return x0, x1


def _majority_price(votes: Sequence[Optional[float]]) -> Optional[float]:
    """多帧投票：取出现 ≥2 帧的一致价格（按分取整）；无多数 → None。"""
    from collections import Counter
    vals = [round(v, 2) for v in votes if v is not None]
    if not vals:
        return None
    val, n = Counter(vals).most_common(1)[0]
    return val if n >= 2 else None


def _price_within_deviation(price: Optional[float], spec: SignalSpec) -> bool:
    """识别价与本次委托价的偏离 ≤ ``_PRICE_DEVIATION_MAX``（默认 2%）。"""
    want = float(spec.price or 0.0)
    if want <= 0 or price is None or float(price) <= 0:
        return False
    return abs(float(price) - want) <= want * _PRICE_DEVIATION_MAX + 1e-9


def _price_within_daily_limits(price: float, spec: SignalSpec) -> bool:
    """识别价须落在当日涨跌停区间内（调用方未提供涨跌停时跳过该条）。"""
    lo = float(getattr(spec, "limit_down", 0.0) or 0.0)
    hi = float(getattr(spec, "limit_up", 0.0) or 0.0)
    if lo > 0 and float(price) < lo - _PRICE_TOL:
        return False
    if hi > 0 and float(price) > hi + _PRICE_TOL:
        return False
    return True


def _fmt_row_seconds(seconds: Optional[int]) -> str:
    """行时间（当日秒数）→ HH:MM:SS；读不出返回 '?'。"""
    if seconds is None:
        return "?"
    s = int(seconds)
    return "%02d:%02d:%02d" % (s // 3600, (s % 3600) // 60, s % 60)


def _describe_unmatched_rows(rows: Sequence[Row],
                             baseline: Optional[Sequence[tuple]],
                             spec: SignalSpec) -> List[str]:
    """列出「下单后新增、同代码、未发送」的候选行，供失败告警与人工核对。

    仅用于记录，**不得**据此自动点击（匹配失败时无法确定哪一行属于本单，
    误点『取消』会撤掉其它真实待发的委托）。字段顺序：代码 方向 数量@价格
    行时间 状态，与 _match_row 的硬条件口径一致，便于人工比对。
    """
    out: List[str] = []
    for row in _subtract_baseline(rows, baseline or ()):
        if spec.code6 and row.code != spec.code6:
            continue
        if row.sent:
            continue    # 已发送的不算残留
        out.append("%s %s %s@%s 行时间 %s（未发送）" % (
            row.code or "?", row.direction or "?", row.volume, row.price,
            _fmt_row_seconds(row.seconds)))
    return out


def _image_has_content(img) -> bool:
    """截图是否含可读内容（全黑/全白 = CEF 硬件渲染没被 PrintWindow 捕获）。"""
    try:
        lo, hi = img.convert("L").resize((64, 64)).getextrema()
        return (hi - lo) >= 25
    except Exception:  # noqa: BLE001
        return True


def _screen_grab(window: WindowRef):
    """按窗口屏幕矩形抓屏（PrintWindow 空白时的兜底，窗口可见才有效）。"""
    try:
        from PIL import ImageGrab
    except ImportError:
        return None
    try:
        if not _user32.IsWindowVisible(wt.HWND(window.hwnd)):
            return None
        img = ImageGrab.grab(bbox=window.rect, all_screens=True)
        return img.convert("RGB")
    except Exception:  # noqa: BLE001
        logger.exception("tdx 交易信号窗口屏幕截图失败")
        return None


def _default_dump_dir() -> Path:
    root = Path(__file__).resolve().parents[4]   # repo root / main/core/engine/live
    candidates = [root / "logs" / "tdx_confirm", Path(tempfile.gettempdir()) / "tdx_confirm"]
    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            return p
        except Exception:  # noqa: BLE001
            continue
    return Path(tempfile.gettempdir()) / "tdx_confirm"


# ---------------------------------------------------------------- 主类


class TdxSignalConfirmer:
    """定位「交易信号」窗口 → OCR 匹配本次委托行 → 点『发送』→ 等委托表出现。"""

    PROCESS_IMAGE = "tdxw.exe"
    TITLE_KEYWORD = "交易信号"

    def __init__(self, timeout: float = 12.0, dump_dir: Optional[str] = None,
                 enable_ocr: bool = True):
        self.timeout = float(timeout)
        self.dump_dir = Path(dump_dir) if dump_dir else _default_dump_dir()
        self.enable_ocr = bool(enable_ocr)
        self._ocr_engine = None
        self._last_img = None
        #: 被本次调用置顶的窗口句柄（0=无）；点击结束由 _restore_topmost 还原
        self._topmost_raised = 0
        #: 最近一次价格列 ROI 探测结果（band/每帧价格/采纳值），随诊断包落盘
        self._price_probe: Optional[dict] = None
        #: 最近一次「强制显示」的时刻（0=从未）：显示后 CEF 仍在重绘，确认流程
        #: 据此在点击前等画面就绪（见 _wait_frame_ready）
        self._force_shown_at = 0.0

    # ---- 公开入口

    def confirm(self, spec: SignalSpec,
                is_submitted: Optional[Callable[[], bool]] = None,
                deadline: Optional[float] = None,
                on_clicked: Optional[Callable[[], None]] = None,
                baseline: Optional[Sequence[tuple]] = None) -> ConfirmOutcome:
        """定位并点击本次委托对应的「发送」，然后等 ``is_submitted`` 为真。

        ``baseline`` 是 ``order_stock`` **之前**由 ``snapshot_rows()`` 拍下的
        旧行指纹；传 None 时在入口现拍（独立调用路径）。基线里的行一律不点，
        只点基线之后新出现的行。返回 ok=True 仅当委托表已见该单（由调用方回调
        确认）；点击本身不作为成功依据。任何异常都收敛为失败结果，不向引擎抛。
        """
        with _dpi_ctx():   # 窗口坐标与鼠标坐标统一到物理像素（见 _dpi_ctx）
            return self._confirm_impl(spec, is_submitted, deadline, on_clicked,
                                      baseline)

    def _confirm_impl(self, spec: SignalSpec,
                      is_submitted: Optional[Callable[[], bool]] = None,
                      deadline: Optional[float] = None,
                      on_clicked: Optional[Callable[[], None]] = None,
                      baseline: Optional[Sequence[tuple]] = None) -> ConfirmOutcome:
        start = time.time()
        end = float(deadline) if deadline else start + self.timeout
        locate_end = min(end, start + self.timeout * 0.65)
        detail = "未找到「交易信号」窗口"
        window: Optional[WindowRef] = None
        clicked = 0
        clicked_row: Optional[Row] = None
        stale = 0
        # 换档 OCR 只重抓一次（每次 confirm 调用）
        alt_tried = False
        # 价格列 ROI 冲突证据是「粘性」的：一旦读出与委托价不符的价，本次
        # confirm 不再走「价格缺失唯一命中」降级放行
        price_conflict = False
        roi_tries = 0
        # 画面就绪等待每次 confirm 只做一次（窗口刚被强制显示时才等）
        ready_waited = False
        self._price_probe = None
        # 最后一次成功读到的行（仅用于失败时记录「可能残留的未发送行」，
        # 绝不据此自动点击——见 _describe_unmatched_rows 的说明）
        last_rows: Sequence[Row] = ()
        if baseline is None:
            baseline = self._snapshot_rows_impl()
        base_n = len(baseline or ())

        while time.time() < locate_end:
            window = self._pick_window()
            if window is None:
                detail = ("未找到标题含「交易信号」的 tdxw.exe 窗口"
                          "（客户端未打开交易信号窗口？）")
                time.sleep(0.35)
                continue
            if not window.visible:
                self._force_show(window)
                time.sleep(0.2)
            # 刚被强制显示的窗口 CEF 仍在重绘（真机：该帧价格列整体缺失，
            # 点击未生效）→ 等「行 + 价格列区间 + 发送按钮」连续两帧稳定再点。
            if (not ready_waited
                    and time.time() - self._force_shown_at <= _READY_GRACE):
                ready_waited = True
                self._force_shown_at = 0.0
                self._wait_frame_ready(
                    window, min(_READY_TIMEOUT,
                                max(0.0, locate_end - time.time())))
            rows = self._read_rows(window)
            last_rows = rows
            if not rows:
                detail = "「交易信号」窗口截图/OCR 无有效文本（渲染未完成或被遮挡）"
                time.sleep(0.4)
                continue
            stale = self._count_matching(rows, spec)
            hit = None
            if _price_unreadable_rows(rows, spec, baseline) and \
                    roi_tries < _PRICE_ROI_MAX_TRIES:
                # 整行整窗 OCR 把价格读丢时，先按列锚点裁价格列 ROI 多帧投票
                # （2026-11-18 真机：64.83 只剩「·83」）；ROI 明确读出价但
                # 与本次委托对不上（约束不满足/价格不符）→ 视为冲突，不再
                # 走「唯一命中」降级放行。再退回换档整窗重抓。
                roi_tries += 1
                hit, conflict = self._recover_hit_via_price_roi(
                    window, rows, spec, baseline)
                price_conflict = price_conflict or conflict
            if hit is None:
                hit = _match_row(rows, spec, baseline=baseline,
                                 allow_price_missing=not price_conflict)
            if hit is None and not alt_tried and \
                    _count_price_unreadable_rows(rows, spec, baseline):
                # 价格列常被 OCR 读丢（2026-11-18 真机：64.83 只剩「·83」）：
                # 换一档放大倍率/阈值重抓一次再判（每次 confirm 只换一次）。
                alt_tried = True
                alt_rows = self._read_rows(window, scale=_OCR_SCALE_ALT,
                                           threshold=_OCR_THRESHOLD_ALT)
                if alt_rows:
                    alt_hit = _match_row(
                        alt_rows, spec, baseline=baseline,
                        allow_price_missing=not price_conflict)
                    if alt_hit is not None:
                        rows, hit = alt_rows, alt_hit
                        last_rows = alt_rows
                        stale = self._count_matching(alt_rows, spec)
            if hit is None:
                if self._scroll_down(window, rows):
                    detail = "待发送行不在可视区，已向下滚动列表"
                    continue
                missing_price = _count_price_unreadable_rows(rows, spec, baseline)
                if price_conflict:
                    detail = ("价格列 ROI 识别出的价格与本次委托不符"
                              "（涨跌停区间/2% 偏离/±1 分不一致），判不可用，未点击")
                elif missing_price:
                    detail = (
                        "OCR 未读出目标行价格，无法唯一确认本次委托行"
                        "（同代码/方向/数量/时间窗候选 %d 条；价格列 ROI 多帧"
                        "投票与换档重抓均未确认），未点击" % missing_price)
                else:
                    detail = ("可视区未见基线之后新出现的本次委托行"
                              "（已排除基线 %d 行；代码/方向/数量/价格/时间均需一致，"
                              "等待渲染）" % base_n)
                time.sleep(0.35)
                continue
            # 点击前最后一次核对坐标落在窗口内
            sx = window.rect[0] + hit.send.cx
            sy = window.rect[1] + hit.send.cy
            method = self._click(window, sx, sy, hit)
            if method == "busy":
                # 跨引擎 UI 互斥被 THS 占用（>2s）：本轮未点击，下一轮重试
                detail = "UI 互斥锁被其它通道占用，本轮未点击（下轮重试）"
                time.sleep(0.3)
                continue
            if not method:
                detail = ("目标窗口无法置顶或『发送』按钮被其它窗口遮挡"
                          "（置顶重试 %d 次仍失败），放弃点击（避免误点）"
                          % _TOPMOST_ATTEMPTS)
                break
            clicked = 1
            clicked_row = hit
            detail = "已点击『发送』，等待委托表出现该单"
            if hit.price is None:
                logger.warning(
                    "tdx 交易信号：命中行价格列 OCR 缺失，按「代码+方向+数量+"
                    "行时间」唯一命中点击（行时间 %s 数量 %s），点击后由委托表"
                    "回查二次核对",
                    _fmt_row_seconds(hit.seconds), hit.volume)
            if hit.code != spec.code6:
                logger.warning(
                    "tdx 交易信号：命中行代码列不可读，按「时间+价格+数量」唯一"
                    "命中降级点击（行时间 %s 价格 %s 数量 %s），点击后由委托表"
                    "回查二次核对",
                    _fmt_row_seconds(hit.seconds), hit.price, hit.volume)
            logger.info(
                "tdx 交易信号自动确认：点击 %s %s x%s @%s 行（行时间 %s，%s）",
                spec.direction, spec.code6, spec.volume, spec.price,
                hit.seconds, method)
            if on_clicked:
                time.sleep(0.5)
                try:
                    on_clicked()
                except Exception:  # noqa: BLE001
                    logger.exception("tdx 下单后置确认钩子异常")
            break

        row_state: Optional[bool] = None
        if clicked and clicked_row is not None:
            row_state = self._verify_clicked_row(window, clicked_row)
            if row_state is False and clicked <= _CLICK_RETRY_MAX:
                # 复核仍读作『发送』：可能点击未生效。重取一帧按同一行补点一次
                # （上限 _CLICK_RETRY_MAX），再复核；仍 False 落回既有失败路径。
                retried = self._reclick_pending_row(
                    window, spec, clicked_row, baseline,
                    allow_price_missing=not price_conflict)
                if retried is not None:
                    clicked += 1
                    clicked_row = retried
                    row_state = self._verify_clicked_row(window, retried)
                    logger.info(
                        "tdx 交易信号：点击后复核仍读作『发送』，已补点一次并复核"
                        "（复核结果=%s）", row_state)

        # 未点击就失败时，客户端「交易信号」窗口可能残留本次委托的未发送行
        # （没点『发送』= 没报出去，但那一行仍可被人工误点）。此处**只记录不点击**：
        # 走到这里的前提是行匹配失败（窗口缺失/OCR 失败/硬条件不符），既然无法
        # 确定哪一行是本单，点任何『取消』都可能撤掉别人真正待发的委托——比留下
        # 一行未发送更危险。故降级为明确告警 + 记录候选行内容，交人工核对。
        if not clicked and last_rows:
            leftovers = _describe_unmatched_rows(last_rows, baseline, spec)
            if leftovers:
                logger.warning(
                    "tdx 交易信号：自动确认未点击，窗口内存在下单后新增的同代码未发送行"
                    "（不会自动点『取消』，以免误取消他人委托；请人工核对后手动取消）：%s",
                    "；".join(leftovers))
                detail = ("%s；窗口内新增未发送行 %d 条"
                          "（需人工核对后手动『取消』）" % (detail, len(leftovers)))

        # 点击结果校验（2026-11-18）：点完『发送』后复查该行是否消失/转为
        # 「已发送」。复核仍为『发送』时已在上方补点一次（上限 1 次）；仍未确认
        # 就按失败返回，由既有「已受理未认领」/deferred 路径兜底——绝不自动取消。
        if clicked and row_state is True:
            return ConfirmOutcome(
                True, "已点击『发送』，该委托行已消失/已发送", clicked,
                stale=stale)

        if clicked and is_submitted is not None:
            while time.time() < end:
                try:
                    if is_submitted():
                        return ConfirmOutcome(True, "委托表已见该单", clicked,
                                              stale=stale)
                except Exception as e:  # noqa: BLE001
                    logger.debug("tdx 确认后回查委托表异常：%s", e)
                time.sleep(0.4)
            if row_state is False:
                detail = ("已点击『发送』，但该行仍为待发送状态，且超时内"
                          "委托表未出现该单")
            else:
                detail = "已点击『发送』，但超时内委托表仍未出现该单（行状态无法核验）"
        elif is_submitted is None and clicked:
            detail = "已点击『发送』（调用方未提供委托表回调，行状态未获确认）"

        self._dump(spec, detail, window, stale)
        return ConfirmOutcome(False, detail, clicked, stale=stale)

    # ---- 窗口

    def _pick_window(self) -> Optional[WindowRef]:
        try:
            enum = _WindowEnumProc()
            items = []
            for hwnd, title, _pid, visible in enum.run():
                if self.TITLE_KEYWORD not in title:
                    continue
                ref = WindowRef(hwnd, title, _window_rect(hwnd), visible,
                                _has_cef_child(hwnd))
                if ref.width <= 0 or ref.height <= 0:
                    continue
                items.append(ref)
            pick = _pick_window_impl(items)
            if pick is not None and len(items) > 1:
                logger.debug("tdx 交易信号窗口候选 %d 个，选 hwnd=%s（可见=%s cef=%s）",
                             len(items), pick.hwnd, pick.visible, pick.has_cef)
            return pick
        except Exception:  # noqa: BLE001
            logger.exception("tdx 枚举交易信号窗口失败")
            return None

    def _force_show(self, window: WindowRef) -> None:
        try:
            if _user32.IsIconic(wt.HWND(window.hwnd)):
                _user32.ShowWindow(wt.HWND(window.hwnd), 9)   # SW_RESTORE
            else:
                _user32.ShowWindow(wt.HWND(window.hwnd), 4)   # SW_SHOWNOACTIVATE
            time.sleep(0.2)
            window.visible = bool(_user32.IsWindowVisible(wt.HWND(window.hwnd)))
            self._force_shown_at = time.time()
            logger.info("tdx 交易信号窗口已隐藏，已强制显示 hwnd=%s", window.hwnd)
        except Exception:  # noqa: BLE001
            logger.exception("tdx 强制显示交易信号窗口失败")

    def _wait_frame_ready(self, window: WindowRef,
                          timeout: Optional[float] = None) -> bool:
        """等「交易信号」画面就绪再点击（2026-11-18）。

        判据：行非空 + 价格列区间可推（``_price_column_band`` 依赖行尾『发送』
        锚点与价格碎片，等价于「价格列 + 发送按钮」都识别出来）+ 连续两帧行指纹
        完全一致。窗口刚被强制显示时 CEF 仍在重绘，固定 sleep 后点击会点空
        （真机：该帧价格列整体缺失、行尾仍是『发送』）。
        超 ``timeout`` 返回 False 并记 WARNING，由调用方按现有路径继续（不改变
        失败语义、不抛异常）。
        """
        limit = _READY_TIMEOUT if timeout is None else max(0.0, float(timeout))
        end = time.time() + limit
        prev_fp: Optional[tuple] = None
        last_n = 0
        last_band: Optional[Tuple[float, float]] = None
        while True:
            try:
                rows = self._read_rows(window)
            except Exception:  # noqa: BLE001
                rows = []
            last_n = len(rows)
            fp = tuple(_row_fingerprint(r) for r in rows)
            band = _price_column_band(rows) if rows else None
            last_band = band
            if rows and band is not None and prev_fp is not None and fp == prev_fp:
                logger.info(
                    "tdx 交易信号：画面就绪（%d 行，价格列 x=%.0f~%.0f，连续两帧一致）",
                    len(rows), band[0], band[1])
                return True
            prev_fp = fp if rows else None
            if time.time() >= end:
                break
            time.sleep(_READY_INTERVAL)
        logger.warning(
            "tdx 交易信号：等待画面就绪超时（%.1fs，行数=%d，价格列区间=%s），"
            "按现有路径继续（可能点击未生效）",
            limit, last_n, "可推" if last_band is not None else "不可推")
        return False

    # ---- 截图 / OCR

    def _grab(self, window: WindowRef):
        try:
            from PIL import Image
        except ImportError:
            return None
        w, h = window.width, window.height
        if w <= 0 or h <= 0:
            return None
        hwnd = wt.HWND(window.hwnd)
        hwnd_dc = _user32.GetWindowDC(hwnd)
        if not hwnd_dc:
            return None
        mem_dc = _gdi32.CreateCompatibleDC(hwnd_dc)
        bmp = _gdi32.CreateCompatibleBitmap(hwnd_dc, w, h)
        old = _gdi32.SelectObject(mem_dc, bmp)
        img = None
        try:
            ok = _user32.PrintWindow(hwnd, mem_dc, 2)   # PW_RENDERFULLCONTENT
            if not ok:
                _gdi32.BitBlt(mem_dc, 0, 0, w, h, hwnd_dc, 0, 0, 0x00CC0020)
            bi = _BITMAPINFOHEADER()
            bi.biSize = ctypes.sizeof(_BITMAPINFOHEADER)
            bi.biWidth = w
            bi.biHeight = -h          # 负数 = 自上而下
            bi.biPlanes = 1
            bi.biBitCount = 32
            bi.biCompression = 0
            buf = ctypes.create_string_buffer(w * h * 4)
            got = _gdi32.GetDIBits(mem_dc, bmp, 0, h, buf,
                                   ctypes.byref(bi), 0)
            if got:
                img = Image.frombuffer("RGB", (w, h), buf, "raw", "BGRX", 0, 1)
                img = img.copy()
        except Exception:  # noqa: BLE001
            logger.exception("tdx 交易信号窗口截图失败")
            img = None
        finally:
            try:
                _gdi32.SelectObject(mem_dc, old)
                _gdi32.DeleteObject(bmp)
                _gdi32.DeleteDC(mem_dc)
                _user32.ReleaseDC(hwnd, hwnd_dc)
            except Exception:  # noqa: BLE001
                pass
        if img is not None and not _image_has_content(img):
            # PrintWindow 对 CEF/DWM 加速窗口常「返回成功但内容全黑」：退化抓
            # 屏幕对应区域（窗口不可见/被遮挡时读不到正确内容 → 定位失败，
            # 点击前 _ensure_topmost 还会再校验一次，不会误点）。
            alt = _screen_grab(window)
            if alt is not None:
                img = alt
        if img is not None:
            self._last_img = img
        return img

    def _ocr(self, img, scale: Optional[int] = None,
             threshold: Optional[int] = None) -> List[tuple]:
        """窗口截图 → 词表（窗口坐标）。scale/threshold 可换档重抓（见调用方）。"""
        if not self.enable_ocr or img is None:
            return []
        scale = int(scale or _OCR_SCALE)
        threshold = int(threshold or _OCR_THRESHOLD)
        try:
            from PIL import Image, ImageOps
        except ImportError:
            return []
        try:
            eng = self._ensure_ocr_engine()
            if eng is None:
                return []
            rgb = img.convert("RGB")
            gray = rgb.convert("L")
            hist_mean = sum(gray.resize((32, 32)).getdata()) / 1024.0
            if hist_mean < 128:          # 深色主题（通达信默认）→ 反色
                rgb = ImageOps.invert(rgb)
            big = rgb.resize((rgb.width * scale, rgb.height * scale),
                             Image.LANCZOS)
            bin_img = big.convert("L").point(
                lambda p: 255 if p > threshold else 0).convert("RGB")
            return self._run_ocr(eng, bin_img, scale=scale)
        except Exception:  # noqa: BLE001
            logger.exception("tdx 交易信号窗口 OCR 失败")
            return []

    def _ensure_ocr_engine(self):
        if self._ocr_engine is not None:
            return self._ocr_engine
        from winrt.windows.globalization import Language
        from winrt.windows.media.ocr import OcrEngine
        eng = None
        try:
            eng = OcrEngine.try_create_from_user_profile_languages()
        except Exception:  # noqa: BLE001
            eng = None
        if eng is None:
            for tag in ("zh-Hans-CN", "zh-CN", "en-US"):
                try:
                    eng = OcrEngine.try_create_from_language(Language(tag))
                except Exception:  # noqa: BLE001
                    continue
                if eng is not None:
                    break
        self._ocr_engine = eng
        return eng

    def _run_ocr(self, eng, img, scale: int = _OCR_SCALE) -> List[tuple]:
        import asyncio
        import tempfile as _tf

        from winrt.windows.graphics.imaging import BitmapDecoder
        from winrt.windows.storage import FileAccessMode, StorageFile

        fd, path = _tf.mkstemp(suffix=".png")
        os.close(fd)
        try:
            img.save(path, "PNG")

            async def _go():
                f = await StorageFile.get_file_from_path_async(path)
                stream = await f.open_async(FileAccessMode.READ)
                dec = await BitmapDecoder.create_async(stream)
                bmp = await dec.get_software_bitmap_async()
                return await eng.recognize_async(bmp)

            loop = asyncio.new_event_loop()
            try:
                res = loop.run_until_complete(_go())
            finally:
                loop.close()
            out = []
            for line in res.lines:
                for w in line.words:
                    b = w.bounding_rect
                    out.append((
                        w.text,
                        float(b.x) / scale,
                        float(b.y) / scale,
                        float(b.width) / scale,
                        float(b.height) / scale,
                    ))
            return out
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    def _read_rows(self, window: WindowRef, scale: Optional[int] = None,
                   threshold: Optional[int] = None) -> List[Row]:
        img = self._grab(window)
        if img is None:
            return []
        return _build_rows(self._ocr(img, scale=scale, threshold=threshold))

    def _ocr_price_roi(self, img, x0: float, y0: float,
                       x1: float, y1: float) -> Optional[float]:
        """裁价格列 ROI 单独 OCR：限定字符集 + 按块自适应阈值（Otsu）。

        返回解析出的价格；裁剪越界/块内无内容/字符集不满足 → None（判不可用，
        绝不用整行结果回填猜测）。
        """
        if not self.enable_ocr or img is None:
            return None
        try:
            from PIL import Image, ImageOps, ImageStat
        except ImportError:
            return None
        try:
            w, h = img.size
            box = (max(0, int(x0)), max(0, int(y0)),
                   min(w, int(x1)), min(h, int(y1)))
            if box[2] - box[0] < 3 or box[3] - box[1] < 3:
                return None
            block = img.crop(box).convert("RGB")
            gray = block.convert("L")
            lo, hi = gray.getextrema()
            if hi - lo < 20:
                return None
            if ImageStat.Stat(gray).mean[0] < 128:
                block = ImageOps.invert(block)   # 深色主题
            big = block.resize((block.width * _PRICE_ROI_SCALE,
                                block.height * _PRICE_ROI_SCALE), Image.LANCZOS)
            gray_big = big.convert("L")
            thr = _otsu_threshold(gray_big)
            if thr is None:
                thr = _OCR_THRESHOLD          # 回退到既有固定阈值
            bin_img = gray_big.point(
                lambda p: 255 if p > thr else 0).convert("RGB")
            eng = self._ensure_ocr_engine()
            if eng is None:
                return None
            words = self._run_ocr(eng, bin_img, scale=_PRICE_ROI_SCALE)
            text = "".join(w[0] for w in sorted(words, key=lambda x: x[1]))
            return _parse_price_block(text)
        except Exception:  # noqa: BLE001
            logger.exception("tdx 价格列 ROI OCR 失败")
            return None

    def _recover_hit_via_price_roi(self, window: WindowRef,
                                   rows: Sequence[Row], spec: SignalSpec,
                                   baseline: Optional[Sequence[tuple]]
                                   ) -> Tuple[Optional[Row], bool]:
        """价格列 ROI 多帧投票 → 回填唯一候选行价格 → 重新匹配。

        返回 ``(命中行, 是否冲突)``：冲突=True 表示 ROI **确实读出了价**，但
        该价不满足「涨跌停区间 / 与委托价偏离 ≤2% / 与委托价一致」，调用方
        必须放弃本行的「价格缺失唯一命中」降级（冲突证据不放行）。读不出价
        或推不出列区间 → ``(None, False)``，由整行路径继续兜底。
        """
        cands = _price_unreadable_rows(rows, spec, baseline)
        probe: dict = {"candidates": len(cands), "band": None,
                       "votes": [], "adopted": None, "accepted": False}
        self._price_probe = probe
        if len(cands) != 1:
            logger.info("tdx 价格列 ROI 跳过：同代码/方向/数量/时间窗候选 %d 条（需唯一）",
                        len(cands))
            return None, False
        band = _price_column_band(rows)
        if band is None:
            logger.info("tdx 价格列 ROI 跳过：当帧推不出价格列区间（回退整行识别）")
            return None, False
        target = cands[0]
        probe["band"] = [round(band[0], 1), round(band[1], 1)]
        y0 = min(c.y for c in target.cells) - _PRICE_ROI_MARGIN_Y
        y1 = max(c.y + c.h for c in target.cells) + _PRICE_ROI_MARGIN_Y
        votes: List[Optional[float]] = []
        for i in range(_PRICE_VOTE_FRAMES):
            votes.append(self._ocr_price_roi(self._grab(window),
                                             band[0], y0, band[1], y1))
            if i < _PRICE_VOTE_FRAMES - 1:
                time.sleep(_PRICE_VOTE_INTERVAL)
        probe["votes"] = votes
        adopted = _majority_price(votes)
        probe["adopted"] = adopted
        logger.info(
            "tdx 价格列 ROI 多帧投票：每帧价格=%s 采纳=%s（列区间 x=%.0f~%.0f）",
            votes, adopted, band[0], band[1])
        if adopted is None:
            # 帧间出现 ≥2 个不同读值 = 证据冲突，不能当「价格读不出」放行
            distinct = set(round(v, 2) for v in votes if v is not None)
            return None, len(distinct) >= 2
        if not _price_within_deviation(adopted, spec):
            logger.warning(
                "tdx 价格列 ROI：采纳价 %s 偏离委托价 %s 超 %.0f%%，判不可用",
                adopted, spec.price, _PRICE_DEVIATION_MAX * 100)
            return None, True
        if not _price_within_daily_limits(adopted, spec):
            logger.warning(
                "tdx 价格列 ROI：采纳价 %s 不在当日涨跌停区间 [%s, %s]，判不可用",
                adopted, spec.limit_down, spec.limit_up)
            return None, True
        target.price_override = adopted
        matched = _match_row(rows, spec, baseline=baseline)
        if matched is None:
            logger.warning(
                "tdx 价格列 ROI：采纳价 %s 与委托价 %s 不一致（±1 分），判冲突不放行",
                adopted, spec.price)
            return None, True
        probe["accepted"] = True
        return matched, False

    def pending_row_state(self, spec: SignalSpec) -> str:
        """该委托在「交易信号」窗口里是否仍停在待发送（2026-11-18）。

        返回 ``"pending"``（唯一命中且行尾仍读作『发送』= 点击未生效/未报出）/
        ``"absent"``（行已消失或已转『已发送』= 视为已报出）/ ``"unknown"``
        （无窗口、OCR 空、多候选无法唯一判定、异常）。仅供引擎在 180s 到期前
        复核，不点击、不取消任何行。
        """
        try:
            with _dpi_ctx():
                window = self._pick_window()
                if window is None:
                    return "unknown"
                rows = self._read_rows(window)
                if not rows:
                    return "unknown"
                state = _pending_unsent_row(rows, spec)
                if state is True:
                    return "pending"
                if state is False:
                    return "absent"
                logger.info("tdx 待发送复核：候选行不唯一，按 unknown 处理（不重排）")
                return "unknown"
        except Exception:  # noqa: BLE001
            logger.exception("tdx 待发送复核异常，按 unknown 处理")
            return "unknown"

    def snapshot_rows(self) -> Tuple[tuple, ...]:
        """``order_stock`` **之前**拍「交易信号」窗口基线（旧行指纹集合）。

        确认流程只允许点基线之后新出现的行；返回空元组表示没拍到（窗口未开/
        无文本/OCR 不可用）——此时匹配退化为「行时间 ≥ 下单时间-2s」，同样
        不会点更早的历史残留行（宁失败不错）。
        """
        try:
            with _dpi_ctx():
                return self._snapshot_rows_impl()
        except Exception:  # noqa: BLE001
            logger.exception("tdx 交易信号窗口基线采集失败")
            return ()

    def _snapshot_rows_impl(self) -> Tuple[tuple, ...]:
        window = self._pick_window()
        if window is None:
            return ()
        if not window.visible:
            self._force_show(window)
            time.sleep(0.2)
        return tuple(_row_fingerprint(r) for r in self._read_rows(window))

    def _verify_clicked_row(self, window: Optional[WindowRef],
                            hit: Row) -> Optional[bool]:
        """点击后校验该行是否消失或转为「已发送」（OCR 可判定时）。

        返回三态：True=已消失 / 行尾已变『已发送』；False=该行仍在且仍读作
        『发送』（点击可能未生效）；None=判不了（无窗口/OCR 读不到），退回
        委托表认领。``is_submitted``（委托表回查）仍是更强的确认口径，二者
        任一确认为真即算报出。
        """
        if window is None:
            logger.info("tdx 确认：点击后无窗口句柄，退回委托表认领判定")
            return None
        try:
            time.sleep(0.5)
            rows = self._read_rows(window)
        except Exception:  # noqa: BLE001
            logger.warning("tdx 确认：点击后 OCR 复核异常，退回委托表认领判定")
            return None
        fp = _row_fingerprint(hit)
        same = [r for r in rows if _row_fingerprint(r) == fp]
        if not same:
            logger.info("tdx 确认：OCR 判定路径=行已消失（已发送并移除）")
            return True
        if any(r.sent for r in same):
            logger.info("tdx 确认：OCR 判定路径=行尾按钮已变『已发送』")
            return True
        # 预期降级：CEF 重绘后 OCR 常把按钮读回『发送』（2026-11-09 实测：
        # 点击后已报柜台 status=50、资金已冻结，OCR 仍报『发送』）。此时不
        # 立即判失败——仍等委托表认领（is_submitted）；超时未认领才按失败返回。
        logger.info("tdx 确认：OCR 复核未见『已发送』（行尾仍读作『发送』或未刷新），"
                    "继续等委托表认领")
        return False

    def _reclick_pending_row(self, window: Optional[WindowRef], spec: SignalSpec,
                             prev: Optional[Row],
                             baseline: Optional[Sequence[tuple]] = None,
                             allow_price_missing: bool = True) -> Optional[Row]:
        """点击后复核仍为『发送』→ 重取一帧、按同一行补点一次（2026-11-18）。

        返回补点成功命中的行；``None`` = 不补点（无窗口/重取失败/行已不可
        唯一匹配/与上次命中不是同一行/点击通道拒绝）。**同一行校验**用
        ``(code, volume, seconds)``，防止补点打到窗口里别的待发委托。
        """
        if window is None or prev is None:
            return None
        try:
            rows = self._read_rows(window)
        except Exception:  # noqa: BLE001
            logger.warning("tdx 确认：补点前重取帧异常，放弃补点")
            return None
        hit = _match_row(rows, spec, baseline=baseline,
                         allow_price_missing=allow_price_missing)
        if hit is None or hit.send is None:
            logger.warning("tdx 确认：补点前未能重新唯一命中同一行，放弃补点")
            return None
        if hit.sent:
            # 该行已转『已发送』（首次点击其实生效了）→ 绝不再点
            return None
        if (hit.code, hit.volume, hit.seconds) != \
                (prev.code, prev.volume, prev.seconds):
            logger.warning(
                "tdx 确认：补点候选与首次命中不是同一行（%s/%s/%s vs %s/%s/%s），"
                "放弃补点", hit.code, hit.volume, hit.seconds,
                prev.code, prev.volume, prev.seconds)
            return None
        sx = window.rect[0] + hit.send.cx
        sy = window.rect[1] + hit.send.cy
        method = self._click(window, sx, sy, hit)
        if method in ("", "busy"):
            logger.warning("tdx 确认：补点未发出（%s），不再重试", method or "遮挡/置顶失败")
            return None
        logger.info("tdx 交易信号：补点『发送』一次（%s，行时间 %s）",
                    method, _fmt_row_seconds(hit.seconds))
        return hit

    # ---- 滚动

    def _scroll_down(self, window: WindowRef, rows_before: Sequence[Row]) -> bool:
        """向下滚动列表；内容有变化返回 True。

        先向 CEF host 投递 WM_MOUSEWHEEL（不抢焦点、不动鼠标），无效再走
        真实滚轮（需窗口在前台，且校验未被遮挡）。
        """
        sig_before = self._row_sig(rows_before)
        host = self._deepest_at(window.hwnd,
                                window.rect[0] + window.width // 2,
                                window.rect[1] + int(window.height * 0.45))
        if host:
            x = window.rect[0] + window.width // 2
            y = window.rect[1] + int(window.height * 0.45)
            lparam = ((y & 0xFFFF) << 16) | (x & 0xFFFF)
            wparam = (((-120 * 3) & 0xFFFF) << 16)
            try:
                _user32.PostMessageW(wt.HWND(host), WM_MOUSEWHEEL,
                                     wt.WPARAM(wparam), wt.LPARAM(lparam))
            except Exception:  # noqa: BLE001
                pass
            time.sleep(0.35)
            rows = self._read_rows(window)
            if self._row_sig(rows) != sig_before:
                return True
        sx = window.rect[0] + window.width // 2
        sy = window.rect[1] + int(window.height * 0.45)
        root = _user32.GetAncestor(
            _user32.WindowFromPoint(wt.POINT(sx, sy)) or window.hwnd, GA_ROOT)
        if int(root) != window.hwnd:
            try:
                _user32.SetForegroundWindow(wt.HWND(window.hwnd))
                time.sleep(0.15)
            except Exception:  # noqa: BLE001
                pass
            root = _user32.GetAncestor(
                _user32.WindowFromPoint(wt.POINT(sx, sy)) or window.hwnd, GA_ROOT)
            if int(root) != window.hwnd:
                return False
        from .win_dialog_scope import UI_AUTO_LOCK
        with UI_AUTO_LOCK:
            _user32.SetCursorPos(int(sx), int(sy))
            _user32.mouse_event(MOUSEEVENTF_WHEEL, 0, 0, -120 * 3, 0)
        time.sleep(0.4)
        rows = self._read_rows(window)
        return self._row_sig(rows) != sig_before

    @staticmethod
    def _row_sig(rows: Sequence[Row]) -> tuple:
        return tuple((r.code, r.volume, r.seconds) for r in rows)

    # ---- 点击

    def _click(self, window: WindowRef, sx: float, sy: float,
               hit: Row) -> str:
        """点击『发送』：真控件 BM_CLICK 优先，否则真实鼠标（CEF 无控件）。"""
        btn = self._button_at(window.hwnd, sx, sy)
        if btn:
            try:
                _user32.SendMessageW(wt.HWND(btn), BM_CLICK, 0, 0)
                return "hwnd_bmclick"
            except Exception:  # noqa: BLE001
                logger.exception("tdx BM_CLICK 失败，改真实点击")
        if not self._ensure_topmost(window, sx, sy):
            return ""
        from .win_dialog_scope import UI_AUTO_LOCK
        # 有超时获取：THS 下单在执行 UI 自动化时最多持有 ~10s，这里不能无限
        # 阻塞把整个 5s 确认预算吃光——锁忙就返回 "busy"，由调用方下一轮重试。
        if not UI_AUTO_LOCK.acquire(timeout=2.0):
            logger.warning("tdx 交易信号点击跳过：UI 互斥锁被占用（>2s），下轮重试")
            self._restore_topmost()
            return "busy"
        try:
            _user32.SetCursorPos(int(sx), int(sy))
            time.sleep(0.05)
            _user32.mouse_event(MOUSEEVENTF_LEFTDOWN, 0, 0, 0, 0)
            time.sleep(0.05)
            _user32.mouse_event(MOUSEEVENTF_LEFTUP, 0, 0, 0, 0)
        finally:
            try:
                UI_AUTO_LOCK.release()
            except Exception:  # noqa: BLE001
                pass
            # 点击完成后立刻还原置顶（无论点击是否被客户端受理），
            # 不让通达信长期悬浮在其它窗口之上。
            self._restore_topmost()
        return "mouse"

    def _ensure_topmost(self, window: WindowRef, sx: float, sy: float) -> bool:
        """把目标窗口提到点击点最上层（防点到遮挡窗口 / 误点其它客户端）。

        主路径 ``SetWindowPos(HWND_TOPMOST)``（后台进程无需前台权限），最小化
        窗口先 ``SW_RESTORE``；每次置顶后用 ``WindowFromPoint`` 复核顶层是否为
        目标窗口，不成立则重试（``_TOPMOST_ATTEMPTS`` 次，间隔 ~200ms）。窗口
        原本已是置顶则不动它；置顶成功后记录 ``_topmost_raised``，由点击结束
        时 ``_restore_topmost`` 还原为 ``HWND_NOTOPMOST``。复核仍不成立 →
        fail-closed 返回 False（绝不按坐标偏移猜测点击）。
        """
        try:
            hwnd = wt.HWND(window.hwnd)
            if _user32.IsIconic(hwnd):
                _user32.ShowWindow(hwnd, 9)   # SW_RESTORE
                time.sleep(0.2)
            if _front_root_at(sx, sy) == window.hwnd:
                return True
            already = _is_topmost(window.hwnd)
            root = _front_root_at(sx, sy)
            for attempt in range(_TOPMOST_ATTEMPTS):
                if not already:
                    _set_topmost(window.hwnd, True)
                    if attempt == 0:
                        self._topmost_raised = window.hwnd
                else:
                    # 已是置顶仍被压住（另一置顶窗口在上）：只能再请求前台
                    try:
                        _user32.SetForegroundWindow(hwnd)
                    except Exception:  # noqa: BLE001
                        pass
                root = _front_root_at(sx, sy)
                if root == window.hwnd:
                    return True
                if attempt < _TOPMOST_ATTEMPTS - 1:
                    time.sleep(_TOPMOST_RETRY_DELAY)
            logger.warning(
                "tdx 交易信号窗口无法置顶/点 (%s,%s) 被其它窗口遮挡"
                "（root=%s hwnd=%s，重试 %d 次）",
                sx, sy, root, window.hwnd, _TOPMOST_ATTEMPTS)
            self._restore_topmost()
            return False
        except Exception:  # noqa: BLE001
            logger.exception("tdx 点击前遮挡校验失败")
            self._restore_topmost()
            return False

    def _restore_topmost(self) -> None:
        """还原点击前被本次调用置顶的窗口（原本就置顶的不动）。"""
        hwnd = self._topmost_raised
        self._topmost_raised = 0
        if not hwnd:
            return
        _set_topmost(hwnd, False)

    def _button_at(self, hwnd: int, sx: float, sy: float) -> Optional[int]:
        """点位内若存在真实 Button 型子控件，返回其句柄（新版/其它主题可能提供）。"""
        stack = [(hwnd, 0)]
        while stack:
            cur, depth = stack.pop()
            if depth > 6:
                continue
            for c in _children(cur):
                r = _window_rect(c)
                if not (r[0] <= sx <= r[2] and r[1] <= sy <= r[3]):
                    continue
                cls = _window_class(c)
                if cls in ("Button", "CCmdButton", "CButton") and \
                        _window_text(c).strip().startswith("发"):
                    return c
                stack.append((c, depth + 1))
        return None

    def _deepest_at(self, hwnd: int, sx: float, sy: float) -> Optional[int]:
        best = None
        stack = [(hwnd, 0)]
        while stack:
            cur, depth = stack.pop()
            if depth > 6:
                continue
            for c in _children(cur):
                r = _window_rect(c)
                if r[0] <= sx <= r[2] and r[1] <= sy <= r[3]:
                    best = c
                    stack.append((c, depth + 1))
        return best

    # ---- 辅助

    def _count_matching(self, rows: Sequence[Row], spec: SignalSpec) -> int:
        return sum(1 for r in rows
                   if r.code == spec.code6 and r.volume == str(int(spec.volume))
                   and (not r.direction or r.direction == spec.direction))

    def _dump(self, spec: SignalSpec, detail: str,
              window: Optional[WindowRef], stale: int) -> None:
        try:
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            folder = self.dump_dir / (
                "%s_%s_%s_%s" % (stamp, spec.code6, spec.direction, spec.volume))
            folder.mkdir(parents=True, exist_ok=True)
            meta = [
                "code6=%s direction=%s volume=%s price=%s sent_at=%s"
                % (spec.code6, spec.direction, spec.volume, spec.price,
                   spec.sent_at),
                "detail=%s" % detail,
                "stale_matching_rows=%d" % stale,
                "price_probe=%r" % (self._price_probe,),
                "window=%r" % (window,),
                "",
                "== tdxw top-level windows ==",
            ]
            try:
                enum = _WindowEnumProc()
                for hwnd, title, pid, visible in enum.run():
                    meta.append("hwnd=%s pid=%s visible=%s class=%r title=%r rect=%s"
                                % (hwnd, pid, visible, _window_class(hwnd),
                                   title, _window_rect(hwnd)))
                    if visible and hwnd == (window.hwnd if window else -1):
                        tree: List[tuple] = []
                        _walk(hwnd, 0, 4, tree)
                        meta.append("  == control tree ==")
                        for d, h, k, t, r, v, cid in tree:
                            meta.append("  %s%s class=%r text=%r id=%s rect=%s vis=%s"
                                        % ("  " * d, hex(h), k, t, cid, r, v))
            except Exception as e:  # noqa: BLE001
                meta.append("window tree dump failed: %s" % e)
            (folder / "meta.txt").write_text("\n".join(meta), encoding="utf-8")
            if window is not None:
                img = self._grab(window)
                if img is not None:
                    img.save(folder / "screenshot.png")
                rows = self._read_rows(window)
                (folder / "ocr_rows.txt").write_text(
                    "\n".join("cy=%.1f code=%r vol=%r dir=%r sec=%s send=%r text=%s"
                              % (r.cy, r.code, r.volume, r.direction, r.seconds,
                                 bool(r.send), " | ".join(c.text for c in r.cells))
                              for r in rows),
                    encoding="utf-8")
            logger.warning("tdx 交易信号自动确认失败，诊断已落盘：%s", folder)
        except Exception:  # noqa: BLE001
            logger.exception("tdx 交易信号失败诊断落盘异常")
