"""THS 验证码 OCR 投票逻辑单元测试（_vote_digits 纯函数）。

THS 验证码恒为 4 位数字：投票只接受恰好 4 位的候选，非 4 位（3 位/6 位
误读）不得参投——否则截断污染多数票，导致「经常出错」。
"""
from core.engine.ths_ui_dispatcher import ThsUiDispatcher


def test_vote_majority_four_digit_wins():
    assert ThsUiDispatcher._vote_digits(["1234", "8234", "1234"]) == "1234"


def test_vote_filters_non_four_digit_candidates():
    """3 位 / 6 位误读截断后不得参投：只剩一个 4 位候选也直接采用。"""
    assert ThsUiDispatcher._vote_digits(["123", "123456", "1234"]) == "1234"


def test_vote_all_invalid_returns_empty():
    assert ThsUiDispatcher._vote_digits(["12", "12345", ""]) == ""


def test_vote_no_majority_takes_first_four_digit():
    assert ThsUiDispatcher._vote_digits(["1111", "2222", "33"]) == "1111"


# ---- THS 数量读回钳制（对齐 project2：min(引擎目标, THS 可买/可卖) + 板块整手）----

def test_clamp_qty_auto_zero_rejects():
    """THS 可买/可卖 = 0 → 拒单（真实资金/持仓不足）。"""
    assert ThsUiDispatcher._clamp_qty(500, 0, "600000.SH", True) is None


def test_clamp_qty_takes_min_and_board_lot():
    """min(引擎 500, THS 可买 350)=350；科创板整手 200 → 200。"""
    assert ThsUiDispatcher._clamp_qty(500, 350, "688083.SH", True) == 200
    assert ThsUiDispatcher._clamp_qty(500, 350, "600000.SH", True) == 300


def test_clamp_qty_auto_none_fallback_engine_qty():
    """读不到（None）→ 引擎量按板块整手兜底。"""
    assert ThsUiDispatcher._clamp_qty(450, None, "600000.SH", True) == 400
    assert ThsUiDispatcher._clamp_qty(450, None, "688083.SH", True) == 400


def test_clamp_qty_sell_no_lot_floor():
    """SELL 不取整手（A 股允许零股卖出），仅 min 钳制。"""
    assert ThsUiDispatcher._clamp_qty(500, 350, "600000.SH", False) == 350


# ---- 验证码键入方式：必须走真实键盘事件（type_keys），不得用 set_edit_text ----
# 2026-11-06 真机实证：验证码弹窗 Edit 是自绘控件，set_edit_text（WM_SETTEXT）
# 不抛异常但输入框保持空白，确定按钮拿不到验证码 → 必须逐字符真实按键。

from types import SimpleNamespace

class _FakeBtn:
    def __init__(self, text):
        self._t = text
        self.clicked = False

    def window_text(self):
        return self._t

    def click(self):
        self.clicked = True


def _mk_captcha_env(monkeypatch, ocr_code="1234"):
    """构造验证码弹窗环境：返回 (dispatcher, edit, dlg, ok_btn)。

    `_find_captcha_dialog` 默认返回 None（视为提交后弹窗已消失）；
    用例按实现的「每轮重新定位弹窗」调用顺序自行编排返回值。
    """
    d = ThsUiDispatcher(captcha_ocr=True, execute_mode="direct")
    edit = SimpleNamespace(
        set_focus=lambda: None,
        type_keys=lambda k, pause=0.0: edit.keys.append(k),
        set_edit_text=lambda t: edit.sets.append(t),
        keys=[], sets=[],
    )
    ok_btn = _FakeBtn("确定")
    dlg = SimpleNamespace(
        set_focus=lambda: None,
        rectangle=lambda: SimpleNamespace(left=0, top=0, right=300, bottom=120),
        descendants=lambda class_name=None: (
            [edit] if class_name == "Edit" else [ok_btn]),
    )
    from PIL import Image as PILImage
    crop = PILImage.new("RGB", (60, 20), (30, 90, 160))

    monkeypatch.setattr(d, "_captcha_crop", lambda dlg_, edit_, rect: crop)
    monkeypatch.setattr(d, "_ocr_digits", lambda img: ocr_code)
    monkeypatch.setattr(d, "_find_captcha_dialog", lambda: None)  # 提交即消失
    monkeypatch.setattr(
        "core.engine.ths_ui_dispatcher.ImageGrab",
        SimpleNamespace(grab=lambda bbox=None: crop))
    return d, edit, dlg, ok_btn


def test_solve_captcha_types_code_with_type_keys(monkeypatch):
    """清空+写入必须走真实按键：set_edit_text 对自绘 Edit 无效（真机实证输入框
    保持空白），实现改为先清空、再逐字符 type_keys。"""
    d, edit, dlg, ok_btn = _mk_captcha_env(monkeypatch)
    state = {"n": 0}

    def _find():
        state["n"] += 1
        return dlg if state["n"] == 1 else None   # 定位到弹窗，提交后消失
    monkeypatch.setattr(d, "_find_captcha_dialog", _find)
    assert d._solve_captcha(dlg) is True
    assert edit.sets == []                       # 不走 WM_SETTEXT（set_edit_text）
    assert edit.keys == ["^a{DELETE}", "1", "2", "3", "4"]   # 清空 + 逐字符键入


def test_solve_captcha_retry_retypes_code(monkeypatch):
    """首错后验证码刷新 → 重试轮重新定位弹窗并重新键入（仍走 type_keys）。"""
    d, edit, dlg, ok_btn = _mk_captcha_env(monkeypatch)
    state = {"n": 0}

    def _find():
        state["n"] += 1
        return dlg if state["n"] <= 3 else None   # 第一次提交后仍在 → 重试
    monkeypatch.setattr(d, "_find_captcha_dialog", _find)
    assert d._solve_captcha(dlg) is True
    assert edit.sets == []
    assert edit.keys == ["^a{DELETE}", "1", "2", "3", "4"] * 2


# ---- TDX「下单确认」弹窗自动点击（无人值守）----

def test_tdx_pick_confirm_dialog_selects_new_dialog(monkeypatch):
    """只挑「新增的、含确认按钮的、属主为通达信」的 #32770 弹窗；
    基线内/无关窗口跳过。"""
    from types import SimpleNamespace
    from core.engine.tdx_tq_dispatcher import TdxTqDispatcher
    from core.engine.live import win_dialog_scope as wds

    # 归属判定统一 fail-closed：显式声明这些弹窗属主为通达信进程
    monkeypatch.setattr(wds, "window_image_name", lambda pid: "tdxw.exe")

    def mk(hwnd, btn_text=None, cls="#32770", visible=True):
        btns = [SimpleNamespace(window_text=lambda t=btn_text: t)] if btn_text else []
        return SimpleNamespace(
            handle=hwnd, class_name=lambda c=cls: c,
            is_visible=lambda: visible,
            process_id=lambda: 100,
            descendants=lambda class_name=None: btns)

    base = mk(1, "设置")           # 基线内 → 跳过
    other = mk(2, None, cls="NotDlg")  # 非 #32770 → 跳过
    target = mk(3, "确定")          # 新增确认框 → 命中

    dlg, btn = TdxTqDispatcher._pick_confirm_dialog(
        [base, other, target], before={1})
    assert dlg is target and btn is not None
    dlg2, btn2 = TdxTqDispatcher._pick_confirm_dialog([other], before=set())
    assert dlg2 is None and btn2 is None


# ---- 双引擎弹窗归属识别（win_dialog_scope）----

def test_dialog_matches_channel_by_image_name(monkeypatch):
    from types import SimpleNamespace
    from core.engine.live import win_dialog_scope as wds

    def mk(pid):
        return SimpleNamespace(process_id=lambda: pid)

    monkeypatch.setattr(wds, "window_image_name", lambda pid: {
        100: "tdxw.exe", 200: "xiadan.exe", 300: "chrome.exe", 400: "",
    }.get(pid, ""))
    assert wds.dialog_matches_channel(mk(100), "tdx") is True      # 通达信
    assert wds.dialog_matches_channel(mk(100), "ths") is False     # 不是同花顺
    assert wds.dialog_matches_channel(mk(200), "ths") is True      # 同花顺
    assert wds.dialog_matches_channel(mk(200), "tdx") is False
    assert wds.dialog_matches_channel(mk(300), "tdx") is False     # 无关进程
    # 2026-11-09 统一为 fail-closed：归属读不出就不点（与主路径 tdxw.exe 严格过滤一致）
    assert wds.dialog_matches_channel(mk(400), "tdx") is False     # 映像名读不出 → 不放行
    assert wds.dialog_matches_channel(mk(200), "iquant") is True   # 未知通道 → 不过滤


def test_tdx_picker_skips_ths_dialog(monkeypatch):
    """TDX 弹窗选择器不点同花顺客户端的确认框（双引擎归属隔离）。"""
    from types import SimpleNamespace
    from core.engine.tdx_tq_dispatcher import TdxTqDispatcher
    from core.engine.live import win_dialog_scope as wds

    ths_dlg = SimpleNamespace(
        handle=9, class_name=lambda: "#32770", is_visible=lambda: True,
        process_id=lambda: 200,
        descendants=lambda class_name=None: [SimpleNamespace(
            window_text=lambda: "确定")])
    monkeypatch.setattr(wds, "window_image_name", lambda pid: "xiadan.exe")

    dlg, btn = TdxTqDispatcher._pick_confirm_dialog([ths_dlg], before=set())
    assert dlg is None and btn is None
