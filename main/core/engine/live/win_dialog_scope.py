"""跨引擎 UI 自动化的弹窗归属识别与互斥（tdx/ths 双引擎并行，2026-11-04）。

两个交易引擎（通达信 TDX / 同花顺 THS）同时运行时，各自的「下单确认/验证码/
提示」弹窗都浮在桌面上。此前各 dispatcher 按桌面全局枚举找弹窗（只过滤
「新增 + 按钮关键词」）——TDX 的确认自动化可能点到 THS 的弹窗（或反向），
鼠标/键盘模拟还会互相抢焦点。

本模块提供两件套：
- ``window_image_name(pid)``：进程映像名（ctypes QueryFullProcessImageName，
  带缓存），供弹窗归属判定；
- ``UI_AUTO_LOCK``：进程级互斥锁——任何鼠标/键盘模拟段落必须持锁执行，
  双引擎的 UI 自动化在此串行（同进程内不同线程）。

通道判定：弹窗属主进程映像名含 "tdx" → 通达信；含 "xiadan"/"hexin" → 同花顺。
"""
from __future__ import annotations

import ctypes
import logging
import threading
from typing import Optional

logger = logging.getLogger(__name__)

# 双引擎 UI 自动化互斥（进程级）：THS _execute_via_ui / TDX _confirm_order_dialog
# 等鼠标键盘段落持锁串行，防焦点/鼠标竞态。
UI_AUTO_LOCK = threading.RLock()

# 通道 → 属主进程映像名关键词（小写）
_CHANNEL_PROC_KEYWORDS = {
    "tdx": ("tdx",),
    "ths": ("xiadan", "hexin"),
}

_pid_image_cache: dict = {}
_cache_lock = threading.Lock()


def window_image_name(pid: int) -> str:
    """进程映像文件名（小写 basename），失败返回空串。带缓存。"""
    if not pid:
        return ""
    with _cache_lock:
        cached = _pid_image_cache.get(pid)
    if cached is not None:
        return cached
    name = ""
    try:
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        k32 = ctypes.windll.kernel32
        h = k32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
        if h:
            try:
                size = 512
                buf = ctypes.create_unicode_buffer(size)
                n = ctypes.c_ulong(size)
                # QueryFullProcessImageNameW（Vista+，不受权限模型限制）
                if k32.QueryFullProcessImageNameW(h, 0, buf, ctypes.byref(n)):
                    full = buf.value
                    name = full.rsplit("\\", 1)[-1].lower()
            finally:
                k32.CloseHandle(h)
    except Exception:  # noqa: BLE001
        name = ""
    # 只缓存成功结果：进程未起来/权限不足导致的空结果若被缓存，会让该 PID 在
    # 整个进程生命周期内永久判为「不属于本通道」（fail-closed 下等于自动化哑火）。
    if name:
        with _cache_lock:
            _pid_image_cache[pid] = name
    return name


def client_window_present(channel: str) -> bool:
    """该通道客户端是否有可见顶层窗口（进程映像名关键词匹配）。

    纯 ctypes、毫秒级：**不 import tqcenter、不初始化 TQ、不碰 UI 自动化**，
    供 watchdog 在盘前/盘中做「不依赖心跳侧证」的在线判定（2026-11-13）。
    与 live_service._window_present 同一口径；未知通道/非 Windows/任何异常 → False。
    """
    keywords = _CHANNEL_PROC_KEYWORDS.get(channel)
    if not keywords:
        return False
    try:
        import ctypes
        from ctypes import wintypes

        found = []
        user32 = ctypes.windll.user32
        enum_proc = ctypes.WINFUNCTYPE(
            wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)

        def _cb(hwnd, _lparam):
            try:
                if user32.IsWindowVisible(hwnd):
                    pid = wintypes.DWORD()
                    user32.GetWindowThreadProcessId(hwnd, ctypes.byref(pid))
                    name = window_image_name(pid.value)
                    if any(k in name for k in keywords):
                        found.append(True)
                        return False
            except Exception:  # noqa: BLE001 - 单个窗口读失败不影响整体
                pass
            return True

        user32.EnumWindows(enum_proc(_cb), 0)
        return bool(found)
    except Exception:  # noqa: BLE001 - 非 Windows/ctypes 异常 → 判不在
        return False


def dialog_matches_channel(dialog, channel: str) -> bool:
    """弹窗是否属于指定通道的客户端进程（映像名关键词匹配）。

    channel 不在已知映射（如 iquant 桥不弹本机窗口）→ 放行 True（不做过滤，
    由调用方原语义）。

    **归属不明一律不放行**（fail-closed）：取 PID 失败或映像名读不出 → False。
    与「主路径 `window_image_name() == "tdxw.exe"`」口径统一，避免双引擎并行
    时把其它客户端的弹窗当成本引擎的确认框点掉（2026-11-09 实测复核时统一）。
    """
    keywords = _CHANNEL_PROC_KEYWORDS.get(channel)
    if not keywords:
        return True
    try:
        pid = dialog.process_id()
    except Exception:  # noqa: BLE001
        return False
    image = window_image_name(pid)
    if not image:
        return False
    return any(k in image for k in keywords)
