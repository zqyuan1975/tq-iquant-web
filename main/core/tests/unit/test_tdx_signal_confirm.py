"""通达信「交易信号」下单确认自动化单测（纯逻辑 + 假窗口，不触真实客户端）。"""
import time as _time
from datetime import datetime

import pytest

from core.engine.live import tdx_signal_confirm as mod
from core.engine.live.tdx_signal_confirm import (
    SignalSpec,
    TdxSignalConfirmer,
    WindowRef,
    _build_rows,
    _match_row,
    _norm_digits,
    _pick_window_impl,
    _row_fingerprint,
)


def _row_words(code, vol, hh, mm, ss, y, direction="卖出", name="测试",
               price="17.05"):
    """按真机 OCR 布局合成一行词表（坐标=窗口内 1x 像素）。"""
    return [
        ("0", 30, y, 8, 11),
        (hh, 70, y, 18, 11), (":", 88, y, 3, 11),
        (mm, 91, y, 18, 11), (":", 109, y, 3, 11),
        (ss, 112, y, 18, 11),
        ("28500208", 160, y, 70, 11),
        (code, 516, y, 52, 11),
        (name, 580, y, 40, 11),
        (direction, 600, y, 20, 11),
        (price, 620, y, 30, 11),
        (vol, 660, y, 30, 11),
        ("发", 677, y, 13, 11), ("送", 689, y, 13, 11),
        ("取", 727, y, 13, 11), ("消", 739, y, 13, 11),
    ]


def _rows(*args):
    words = []
    for item in args:
        words.extend(item)
    return _build_rows(words)


SPEC = SignalSpec(code6="002056", direction="sell", volume=300,
                  price=17.05, sent_at=datetime(2026, 11, 4, 14, 52, 42))


def test_norm_digits_handles_ocr_confusions():
    assert _norm_digits("0020S6") == "002056"
    assert _norm_digits("60O888") == "600888"
    assert _norm_digits("lOO") == "100"


def test_build_rows_groups_by_y_and_exposes_row_fields():
    """只按 y 聚行（列不合并）：语义（时间/按钮）由 Row 属性按 x 邻近精确处理。"""
    rows = _rows(_row_words("0020S6", "300", "14", "43", "09", 70))
    assert len(rows) == 1
    row = rows[0]
    assert row.code == "002056"         # S→5 归一；时间单元格不参与代码判定
    assert row.volume == "300"
    assert row.direction == "sell"
    assert row.seconds == 14 * 3600 + 43 * 60 + 9   # 时间碎片按 x 拼接后取正则
    assert row.send is not None and row.send.text == "发送"


def test_match_row_prefers_time_window_and_bottom_most():
    rows = _rows(
        _row_words("0020S6", "300", "14", "43", "09", 70),
        _row_words("0020S6", "300", "14", "51", "20", 170),
        _row_words("0020S6", "300", "14", "52", "42", 220),
    )
    hit = _match_row(rows, SPEC)
    assert hit is not None
    assert hit.seconds == 14 * 3600 + 52 * 60 + 42


def test_match_row_picks_closest_time_when_two_rows_in_window():
    """同代码/数量/方向的两笔（时间窗内）→ 取时间最接近本次下单的那行。

    连续两笔止损（两个组合都卖同一只）时，不能因上一笔行还没消失就点它。
    """
    spec = SignalSpec(code6="002056", direction="sell", volume=300, price=17.05,
                      sent_at=datetime(2026, 11, 4, 14, 52, 46))
    rows = _rows(
        _row_words("0020S6", "300", "14", "52", "42", 70),
        _row_words("0020S6", "300", "14", "52", "46", 220),
    )
    hit = _match_row(rows, spec)
    assert hit is not None
    assert hit.seconds == 14 * 3600 + 52 * 60 + 46


def test_match_row_rejects_stale_rows_outside_time_window():
    rows = _rows(_row_words("0020S6", "300", "14", "49", "33", 70))
    assert _match_row(rows, SPEC) is None


def test_match_row_rejects_volume_mismatch():
    rows = _rows(_row_words("0020S6", "100", "14", "52", "42", 70))
    assert _match_row(rows, SPEC) is None


def test_match_row_rejects_wrong_direction():
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 70,
                            direction="买入"))
    assert _match_row(rows, SPEC) is None


def test_match_row_without_time_only_accepts_single_candidate():
    spec = SignalSpec(code6="002056", direction="sell", volume=300)
    one = _rows(_row_words("0020S6", "300", "14", "52", "42", 70))
    assert _match_row(one, spec) is not None
    two = _rows(
        _row_words("0020S6", "300", "14", "43", "09", 70),
        _row_words("0020S6", "300", "14", "52", "42", 220),
    )
    assert _match_row(two, spec) is None


def test_match_row_uses_send_button_not_cancel():
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 70))
    send = rows[0].send
    assert send is not None
    assert send.cx < 700   # 「发送」在「取消」左侧


def test_pick_window_prefers_visible_cef():
    hidden_cef = WindowRef(1, "交易信号", (0, 0, 800, 450), False, True)
    visible_no_cef = WindowRef(2, "交易信号", (0, 0, 800, 450), True, False)
    visible_cef = WindowRef(3, "交易信号", (0, 0, 800, 450), True, True)
    pick = _pick_window_impl([hidden_cef, visible_no_cef, visible_cef])
    assert pick.hwnd == 3
    assert _pick_window_impl([hidden_cef, visible_no_cef]).hwnd == 2
    assert _pick_window_impl([hidden_cef]).hwnd == 1
    assert _pick_window_impl([]) is None


def _fake_confirmer(monkeypatch, rows, visible=True, click_method="mouse",
                    dump_dir=None):
    c = TdxSignalConfirmer(timeout=0.5, dump_dir=dump_dir)
    win = WindowRef(100, "交易信号", (0, 0, 800, 500), visible, True)
    monkeypatch.setattr(c, "_pick_window", lambda: win)
    # 换档重抓会带 scale/threshold 关键字参数
    monkeypatch.setattr(c, "_read_rows", lambda _w, **_kw: rows)
    monkeypatch.setattr(c, "_scroll_down", lambda _w, _r: False)
    clicks = []

    def _click(window, sx, sy, hit):
        clicks.append((sx, sy))
        return click_method

    monkeypatch.setattr(c, "_click", _click)
    monkeypatch.setattr("core.engine.live.tdx_signal_confirm.time.sleep",
                        lambda _s: None)
    return c, clicks


def test_confirm_clicks_and_waits_for_submission(monkeypatch, tmp_path):
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 220))
    c, clicks = _fake_confirmer(monkeypatch, rows, dump_dir=str(tmp_path))
    state = {"n": 0}

    def submitted():
        state["n"] += 1
        return state["n"] >= 3

    out = c.confirm(SPEC, is_submitted=submitted, deadline=_time.time() + 1.0,
                    baseline=())
    assert out.ok is True
    # 复核仍读作『发送』→ 补点一次（2026-11-18 改动 2），上限 2 次
    assert len(clicks) == 2 and out.clicks == 2
    # 点击坐标 = 窗口内「发送」按钮中心（相对窗口偏移）
    assert 670 <= clicks[0][0] <= 700 and 210 <= clicks[0][1] <= 235


def test_confirm_does_not_click_when_occluded(monkeypatch, tmp_path):
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 220))
    c, clicks = _fake_confirmer(monkeypatch, rows, click_method="",
                                dump_dir=str(tmp_path))
    out = c.confirm(SPEC, is_submitted=lambda: False,
                    deadline=_time.time() + 0.3, baseline=())
    assert out.ok is False
    # 假 _click 会记录「尝试坐标」，但遮挡时不得计入已点击次数
    assert out.clicks == 0
    assert "遮挡" in out.detail


def test_confirm_failure_dumps_diagnostics(monkeypatch, tmp_path):
    c = TdxSignalConfirmer(timeout=0.2, dump_dir=str(tmp_path))
    monkeypatch.setattr(c, "_pick_window", lambda: None)
    monkeypatch.setattr("core.engine.live.tdx_signal_confirm.time.sleep",
                        lambda _s: None)
    out = c.confirm(SPEC, is_submitted=lambda: False,
                    deadline=_time.time() + 0.1, baseline=())
    assert out.ok is False
    assert "交易信号" in out.detail
    dumps = list(tmp_path.glob("*/meta.txt"))
    assert dumps, "失败必须落盘诊断"
    assert "code6=002056" in dumps[0].read_text(encoding="utf-8")


# ---------------------------------------------------------------- 加固（2026-11-05）


def test_row_price_reads_split_ocr_fragments():
    """真机 OCR 会把 17.64 拆成「1」「7」「·」「64」——必须按 x 邻近拼接。"""
    words = [
        ("0", 30, 70, 8, 11),
        ("09", 70, 70, 18, 11), ("：", 88, 70, 3, 11),
        ("51", 91, 70, 18, 11), ("：", 109, 70, 3, 11),
        ("08", 112, 70, 18, 11),
        ("28500208", 160, 70, 70, 11),
        ("0020S6", 516, 70, 52, 11),
        ("测试", 580, 70, 40, 11),
        ("卖出", 600, 70, 20, 11),
        ("1", 620, 70, 7, 11), ("7", 628, 70, 7, 11),
        ("·", 636, 70, 4, 11), ("64", 641, 70, 14, 11),
        ("300", 675, 70, 30, 11),
        ("发", 700, 70, 13, 11), ("送", 712, 70, 13, 11),
        ("取", 750, 70, 13, 11), ("消", 762, 70, 13, 11),
    ]
    row = _build_rows(words)[0]
    assert abs(row.price - 17.64) <= 1e-9


def test_match_row_excludes_baseline_even_if_old_row_time_closer():
    """基线里的旧行一律排除：即使它离 sent_at 更近，也只点基线之后的新行。"""
    old = _rows(_row_words("0020S6", "300", "14", "52", "42", 70))
    new = _rows(_row_words("0020S6", "300", "14", "52", "43", 220))
    baseline = tuple(_row_fingerprint(r) for r in old)
    rows = old + new
    hit = _match_row(rows, SPEC, baseline=baseline)
    assert hit is not None
    assert hit.seconds == 14 * 3600 + 52 * 60 + 43
    # 对照：不传基线时旧逻辑会点更近的旧行（这正是 09:51 误点的成因）
    assert _match_row(rows, SPEC).seconds == 14 * 3600 + 52 * 60 + 42


def test_match_row_requires_price_match():
    """同代码/数量/方向、不同价格的两行（分批减仓），只点价格一致的那行。"""
    rows = _rows(
        _row_words("0020S6", "300", "14", "52", "42", 70, price="17.05"),
        _row_words("0020S6", "300", "14", "52", "42", 150, price="16.98"),
    )
    hit = _match_row(rows, SPEC)
    assert hit is not None
    assert abs(hit.price - 17.05) <= 0.011
    # 价格读不出（市价/OCR 缺失）且当帧唯一命中「代码+方向+数量+时间」→ 认这行
    opaque = _rows(_row_words("0020S6", "300", "14", "52", "42", 70,
                              price="市价"))
    assert _match_row(opaque, SPEC) is not None
    # 价格读不出且有两行候选（无法唯一确定）→ 不猜、不点
    two = _rows(
        _row_words("0020S6", "300", "14", "52", "42", 70, price="市价"),
        _row_words("0020S6", "300", "14", "52", "42", 220, price="市价"),
    )
    assert _match_row(two, SPEC) is None


def test_match_row_price_missing_requires_unique_time_match():
    """价格缺失兜底必须「代码+方向+数量+行时间」唯一：时间窗外的同代码行不算。"""
    rows = _rows(
        _row_words("0020S6", "300", "14", "52", "42", 70, price="市价"),
        _row_words("0020S6", "300", "09", "35", "00", 220, price="市价"),
    )
    hit = _match_row(rows, SPEC)
    assert hit is not None
    assert hit.seconds == 14 * 3600 + 52 * 60 + 42
    # 同代码/数量但数量不同 → 不参与候选
    other_vol = _rows(
        _row_words("0020S6", "300", "14", "52", "42", 70, price="市价"),
        _row_words("0020S6", "100", "14", "52", "42", 220, price="市价"),
    )
    assert _match_row(other_vol, SPEC) is not None


def test_confirm_never_clicks_old_row_when_new_row_not_rendered(
        monkeypatch, tmp_path):
    """新行未渲染时：旧行虽在基线外的时间窗内，也不得点击，直接判失败落盘。"""
    old = _rows(_row_words("0020S6", "300", "14", "52", "42", 70))
    c, clicks = _fake_confirmer(monkeypatch, old, dump_dir=str(tmp_path))
    baseline = tuple(_row_fingerprint(r) for r in old)
    out = c.confirm(SPEC, is_submitted=lambda: False,
                    deadline=_time.time() + 0.3, baseline=baseline)
    assert out.ok is False
    assert out.clicks == 0
    assert clicks == []          # 一次都没尝试点旧行
    assert "基线" in out.detail
    dumps = list(tmp_path.glob("*/meta.txt"))
    assert dumps, "新行未渲染也必须落盘诊断"
    assert "stale_matching_rows=1" in dumps[0].read_text(encoding="utf-8")


def test_confirm_reports_leftover_row_without_clicking(monkeypatch, tmp_path, caplog):
    """未点击即失败：只告警并记录候选行内容（代码/方向/数量/价格/行时间），
    绝不自动点『取消』——匹配失败时无法确定哪一行属于本单。"""
    # 同代码/同量/同价，但行时间 09:35 远早于下单时间 14:52:42 → 不匹配、不点击
    rows = _rows(_row_words("0020S6", "300", "09", "35", "00", 220))
    c, clicks = _fake_confirmer(monkeypatch, rows, dump_dir=str(tmp_path))
    with caplog.at_level("WARNING", logger="core.engine.live.tdx_signal_confirm"):
        out = c.confirm(SPEC, is_submitted=lambda: False,
                        deadline=_time.time() + 0.3, baseline=())
    assert out.ok is False
    assert out.clicks == 0
    assert clicks == []                     # 含『取消』在内一次都没点
    assert "未发送行" in out.detail
    text = caplog.text
    assert "002056" in text and "300@17.05" in text and "09:35:00" in text


# ------------------------------------------------ 跨引擎 UI 互斥（2026-11-06 P1）


def test_click_returns_busy_when_ui_lock_held(monkeypatch):
    """UI 互斥被 THS 占用：不阻塞、不点击，返回 "busy" 交由调用方下轮重试。"""
    from core.engine.live import win_dialog_scope

    c = TdxSignalConfirmer(timeout=0.2, dump_dir=None)
    monkeypatch.setattr(c, "_ensure_topmost", lambda w, x, y: True)
    monkeypatch.setattr(c, "_button_at", lambda hwnd, x, y: None)

    class _Lock:
        def acquire(self, timeout=None):
            assert timeout is not None and timeout <= 2.0
            return False

        def release(self):
            raise AssertionError("未取到锁不得释放")

    monkeypatch.setattr(win_dialog_scope, "UI_AUTO_LOCK", _Lock())
    win = WindowRef(100, "交易信号", (0, 0, 800, 500), True, True)
    assert c._click(win, 10, 10, None) == "busy"


def test_confirm_retries_when_ui_lock_busy(monkeypatch, tmp_path):
    """锁忙不是「被遮挡放弃」：confirm 应继续下一轮，而不是 break。"""
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 220))
    c, clicks = _fake_confirmer(monkeypatch, rows, click_method="busy",
                               dump_dir=str(tmp_path))
    out = c.confirm(SPEC, is_submitted=lambda: False,
                    deadline=_time.time() + 0.2, baseline=())
    assert out.ok is False
    assert out.clicks == 0
    assert len(clicks) >= 2, "锁忙应重试，而不是一次就放弃"
    assert "互斥" in out.detail


# ---------------------------------------------------------------- 真机回归（2026-11-10）


def _incident_row_20261110(*, with_code=True):
    """真机 2026-11-10 09:31:46 落盘的 OCR 行（logs/tdx_confirm/...001311.../ocr_rows.txt）。

    关键缺陷形态：代码列被拆成「001 | 31 | 1」、方向列整个漏识别、价格拆成
    「1 | 4 | · | 95」——单格 code 判 "" 导致 ``_match_row`` 判「无本单行」放弃，
    委托行滞留在「交易信号」窗口未发送。with_code=False 时连代码碎片也没有
    （走「时间+价格+数量唯一命中」降级路径）。
    """
    y = 60.0
    words = [
        ("0", 30, y, 8, 11),
        ("09", 70, y, 18, 11), ("：", 88, y, 3, 11),
        ("31", 91, y, 18, 11), ("：", 109, y, 3, 11),
        ("37", 112, y, 18, 11),
        ("28S00208", 160, y, 70, 11),
    ]
    if with_code:
        words += [("001", 402, y, 18, 11), ("31", 421, y, 18, 11),
                  ("1", 441, y, 6, 11)]
    words += [
        ("多", 470, y, 14, 11), ("利", 484, y, 14, 11),
        ("科", 498, y, 14, 11), ("技", 512, y, 14, 11),
        # 间距 > 8px 会让 _row_text 插空格 → 正则只能读到 4·95
        ("1", 620, y, 6, 11), ("4", 640, y, 6, 11),
        ("·", 654, y, 4, 11), ("95", 664, y, 12, 11),
        ("3600", 690, y, 30, 11),
        ("发", 740, y, 13, 11), ("送", 752, y, 13, 11),
        ("取", 790, y, 13, 11), ("消", 802, y, 13, 11),
    ]
    return words


INCIDENT_SPEC = SignalSpec(
    code6="001311", direction="buy", volume=3600, price=14.95,
    sent_at=datetime(2026, 11, 10, 9, 31, 37),
)


def test_row_reassembles_code_split_by_ocr_20261110():
    """001|31|1 → 001311；14|·|95 → 14.95（价格间距大，正则只读到 4·95）。"""
    row = _build_rows(_incident_row_20261110())[0]
    assert row.code == "001311"
    assert row.volume == "3600"
    assert row.direction == ""                 # 方向列整列漏识别
    assert abs(row.price - 14.95) <= 1e-9


def test_match_row_hits_incident_ocr_row_20261110():
    """回归：该真机 OCR 行必须被匹配到（原先 code 判空 → 自动确认放弃）。"""
    rows = _rows(_incident_row_20261110())
    hit = _match_row(rows, INCIDENT_SPEC)
    assert hit is not None
    assert hit.code == "001311"
    assert hit.seconds == 9 * 3600 + 31 * 60 + 37


def test_match_row_loose_degrades_only_when_unique():
    """代码列完全读不出时：时间+价格+数量唯一命中才降级放行，多候选一律不放。"""
    one = _rows(_incident_row_20261110(with_code=False))
    assert one[0].code == ""
    spec = SignalSpec(code6="001311", direction="buy", volume=3600,
                      price=14.95, sent_at=datetime(2026, 11, 10, 9, 31, 37))
    assert _match_row(one, spec) is not None

    # 两行都是同一时间/价格/数量（无法区分）→ 不猜、不点
    shifted = [(t, x, y + 100, w, h)
               for (t, x, y, w, h) in _incident_row_20261110(with_code=False)]
    two = _rows(_incident_row_20261110(with_code=False), shifted)
    assert len(two) == 2
    assert _match_row(two, spec) is None

    # 价格不符 → 不降级
    spec2 = SignalSpec(code6="001311", direction="buy", volume=3600,
                       price=15.20, sent_at=datetime(2026, 11, 10, 9, 31, 37))
    assert _match_row(one, spec2) is None


# -------------------------------------- 真机回归（2026-11-18 置顶 / 价格缺失）


def test_ensure_topmost_uses_setwindowpos_and_retries(monkeypatch):
    """SetWindowPos 为主：第一次仍被遮挡 → 置顶重试，复核通过即放行。"""
    c = TdxSignalConfirmer(timeout=0.2, dump_dir=None)
    win = WindowRef(100, "交易信号", (0, 0, 800, 500), True, True)
    roots = [788804, 100]          # 第一次：平台 Edge 窗口压住；重试后归位
    monkeypatch.setattr(mod, "_front_root_at",
                        lambda _x, _y: roots.pop(0) if roots else 100)
    monkeypatch.setattr(mod, "_is_topmost", lambda _h: False)
    calls = []
    monkeypatch.setattr(mod, "_set_topmost",
                        lambda h, top: calls.append((h, top)) or True)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    assert c._ensure_topmost(win, 900, 300) is True
    assert calls[0] == (100, True)          # 置顶过
    assert c._topmost_raised == 100
    c._restore_topmost()
    assert calls[-1] == (100, False)        # 点击结束必须还原
    assert c._topmost_raised == 0


def test_ensure_topmost_fails_closed_after_retries(monkeypatch):
    """三次置顶复核都失败 → fail-closed 放弃（绝不按坐标偏移猜测）。"""
    c = TdxSignalConfirmer(timeout=0.2, dump_dir=None)
    win = WindowRef(100, "交易信号", (0, 0, 800, 500), True, True)
    monkeypatch.setattr(mod, "_front_root_at", lambda _x, _y: 788804)
    monkeypatch.setattr(mod, "_is_topmost", lambda _h: False)
    calls = []
    monkeypatch.setattr(mod, "_set_topmost",
                        lambda h, top: calls.append((h, top)) or True)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    assert c._ensure_topmost(win, 900, 300) is False
    assert sum(1 for _h, top in calls if top) == mod._TOPMOST_ATTEMPTS
    assert calls[-1] == (100, False)        # 失败也还原，不留置顶残留
    assert c._topmost_raised == 0


def test_ensure_topmost_leaves_already_topmost_window_untouched(monkeypatch):
    """窗口原本已置顶（用户自己设的）：不重复改状态、失败也不取消置顶。"""
    c = TdxSignalConfirmer(timeout=0.2, dump_dir=None)
    win = WindowRef(100, "交易信号", (0, 0, 800, 500), True, True)
    monkeypatch.setattr(mod, "_front_root_at", lambda _x, _y: 788804)
    monkeypatch.setattr(mod, "_is_topmost", lambda _h: True)
    calls = []
    monkeypatch.setattr(mod, "_set_topmost",
                        lambda h, top: calls.append((h, top)) or True)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    assert c._ensure_topmost(win, 900, 300) is False
    assert not [x for x in calls if x[1] is True]
    assert c._topmost_raised == 0


def test_click_restores_topmost_after_mouse_click(monkeypatch):
    """鼠标点击路径结束后必须撤销本次置顶（通达信不长期悬浮）。"""
    from core.engine.live import win_dialog_scope

    c = TdxSignalConfirmer(timeout=0.2, dump_dir=None)
    win = WindowRef(100, "交易信号", (0, 0, 800, 500), True, True)
    monkeypatch.setattr(c, "_ensure_topmost", lambda w, x, y: True)
    monkeypatch.setattr(c, "_button_at", lambda hwnd, x, y: None)
    c._topmost_raised = 100

    class _Lock:
        def acquire(self, timeout=None):
            return True

        def release(self):
            pass

    monkeypatch.setattr(win_dialog_scope, "UI_AUTO_LOCK", _Lock())

    class _U:
        def SetCursorPos(self, _x, _y):
            return True

        def mouse_event(self, *a):
            pass

    monkeypatch.setattr(mod, "_user32", _U())
    calls = []
    monkeypatch.setattr(mod, "_set_topmost",
                        lambda h, top: calls.append((h, top)) or True)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    assert c._click(win, 10, 10, None) == "mouse"
    assert (100, False) in calls
    assert c._topmost_raised == 0


def test_confirm_retries_with_alt_ocr_scale_when_price_unreadable(
        monkeypatch, tmp_path):
    """价格列读不出且不唯一 → 换档（放大倍率/阈值）重抓一次再判。"""
    rows_opaque = _rows(
        _row_words("0020S6", "300", "14", "52", "42", 70, price="市价"),
        _row_words("0020S6", "300", "14", "52", "42", 220, price="市价"),
    )
    rows_ok = _rows(_row_words("0020S6", "300", "14", "52", "42", 220))
    c = TdxSignalConfirmer(timeout=0.5, dump_dir=str(tmp_path))
    win = WindowRef(100, "交易信号", (0, 0, 800, 500), True, True)
    monkeypatch.setattr(c, "_pick_window", lambda: win)
    scales = []

    def _read(_w, scale=None, threshold=None):
        scales.append(scale)
        return rows_ok if scale else rows_opaque

    monkeypatch.setattr(c, "_read_rows", _read)
    monkeypatch.setattr(c, "_scroll_down", lambda _w, _r: False)
    monkeypatch.setattr(c, "_click", lambda w, x, y, hit: "mouse")
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    out = c.confirm(SPEC, is_submitted=lambda: True,
                    deadline=_time.time() + 0.5, baseline=())
    assert any(s for s in scales if s), "价格读不出时必须换档重抓一次"
    assert out.ok is True


def test_confirm_reports_price_missing_distinctly(monkeypatch, tmp_path):
    """失败文案必须区分「OCR 未读出价格」与「可视区未见委托行」。"""
    rows = _rows(
        _row_words("0020S6", "300", "14", "52", "42", 70, price="市价"),
        _row_words("0020S6", "300", "14", "52", "42", 220, price="市价"),
    )
    c, clicks = _fake_confirmer(monkeypatch, rows, dump_dir=str(tmp_path))
    out = c.confirm(SPEC, is_submitted=lambda: False,
                    deadline=_time.time() + 0.2, baseline=())
    assert out.ok is False
    assert out.clicks == 0
    assert clicks == []
    assert "价格" in out.detail
    assert "可视区未见" not in out.detail


def test_confirm_success_when_clicked_row_disappears(monkeypatch, tmp_path):
    """点击后该行消失（已发送并移除）→ 无需等委托表即算报出。"""
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 220))
    c = TdxSignalConfirmer(timeout=0.4, dump_dir=str(tmp_path))
    win = WindowRef(100, "交易信号", (0, 0, 800, 500), True, True)
    state = {"clicked": False}
    monkeypatch.setattr(c, "_pick_window", lambda: win)

    def _read(_w, **_kw):
        return [] if state["clicked"] else rows

    def _click(_w, _x, _y, _hit):
        state["clicked"] = True
        return "mouse"

    monkeypatch.setattr(c, "_read_rows", _read)
    monkeypatch.setattr(c, "_scroll_down", lambda _w, _r: False)
    monkeypatch.setattr(c, "_click", _click)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    out = c.confirm(SPEC, is_submitted=lambda: False,
                    deadline=_time.time() + 0.4, baseline=())
    assert out.ok is True
    assert out.clicks == 1
    assert "消失" in out.detail


def test_confirm_fails_when_clicked_row_still_pending(monkeypatch, tmp_path):
    """点击后该行仍待发送且委托表未出现 → 按失败返回（交由既有兜底路径）。"""
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 220))
    c, clicks = _fake_confirmer(monkeypatch, rows, dump_dir=str(tmp_path))
    out = c.confirm(SPEC, is_submitted=lambda: False,
                    deadline=_time.time() + 0.2, baseline=())
    assert out.ok is False
    # 首次点击 + 复核仍读作『发送』→ 补点一次（改动 2）
    assert out.clicks == 2
    assert "待发送" in out.detail
    dumps = list(tmp_path.glob("*/meta.txt"))
    assert dumps, "点击后确认失败也必须落盘诊断"


# --------------------------------- 价格列 ROI 单独识别（2026-11-18 加固）


def test_parse_price_block_restricts_charset():
    """价格块只留数字/小数点/负号；丢字、多段数字、双负号一律不可用。"""
    assert mod._parse_price_block("64.83") == 64.83
    assert mod._parse_price_block("6 4 . 8 3") == 64.83
    assert mod._parse_price_block("64.83元") == 64.83
    assert mod._parse_price_block("·83") is None        # 缺整数位（真机丢「64」）
    assert mod._parse_price_block("64.83532") is None   # 拼进数量等多段数字
    assert mod._parse_price_block("--64.83") is None


def test_otsu_threshold_adapts_per_block_and_falls_back():
    PIL = pytest.importorskip("PIL.Image")
    img = PIL.new("L", (40, 10), 20)
    for x in range(20, 40):
        for y in range(10):
            img.putpixel((x, y), 230)
    thr = mod._otsu_threshold(img)
    assert thr is not None and 20 <= thr < 230
    assert mod._otsu_threshold(PIL.new("L", (40, 10), 128)) is None


def test_price_column_band_derived_from_row_anchors():
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 220, price="·83"))
    band = mod._price_column_band(rows)
    assert band is not None
    x0, x1 = band
    assert x0 <= 620 and x1 >= 650      # 覆盖完整价格块（含被丢掉的整数位）
    assert x1 <= 660                     # 不吃进数量列


def test_price_column_band_none_without_price_fragments():
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 220, price="市价"))
    assert mod._price_column_band(rows) is None


def test_majority_price_requires_two_agreeing_frames():
    assert mod._majority_price([17.05, None, 17.05]) == 17.05
    assert mod._majority_price([17.05, 17.06, None]) is None
    assert mod._majority_price([None, None, None]) is None


def test_price_constraints_reject_implausible_values():
    spec = SignalSpec(code6="301600", direction="sell", volume=532,
                      price=17.05, sent_at=datetime(2026, 11, 18, 9, 31, 41))
    assert mod._price_within_deviation(17.05, spec)
    assert mod._price_within_deviation(17.30, spec)       # +1.47% 合法
    assert not mod._price_within_deviation(20.00, spec)   # +17% 不可用
    assert mod._price_within_daily_limits(17.05, spec)    # 未给涨跌停 → 跳过
    limited = SignalSpec(code6="301600", direction="sell", volume=532,
                         price=17.05, sent_at=datetime(2026, 11, 18, 9, 31, 41),
                         limit_up=17.10, limit_down=17.00)
    assert mod._price_within_daily_limits(17.05, limited)
    assert not mod._price_within_daily_limits(17.30, limited)


def _roi_confirmer(monkeypatch, rows, votes, dump_dir=None):
    """价格列 ROI 路径的假确认器：价格帧按 votes 循环返回（便于断言）。"""
    from itertools import cycle

    c = TdxSignalConfirmer(timeout=0.5, dump_dir=dump_dir)
    win = WindowRef(100, "交易信号", (0, 0, 800, 500), True, True)
    monkeypatch.setattr(c, "_pick_window", lambda: win)
    monkeypatch.setattr(c, "_read_rows", lambda _w, **_kw: rows)
    monkeypatch.setattr(c, "_scroll_down", lambda _w, _r: False)
    monkeypatch.setattr(c, "_grab", lambda _w: None)
    monkeypatch.setattr(c, "_click", lambda w, x, y, hit: "mouse")
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    seq = cycle(list(votes))
    calls = []

    def _ocr(_img, x0, y0, x1, y1):
        calls.append((x0, y0, x1, y1))
        return next(seq)

    monkeypatch.setattr(c, "_ocr_price_roi", _ocr)
    return c, calls


def test_confirm_adopts_roi_voted_price_when_row_price_missing(
        monkeypatch, tmp_path):
    """整行价格读不出 → 价格列 ROI 多帧投票采纳后唯一命中并点击。"""
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 220, price="·83"))
    c, calls = _roi_confirmer(monkeypatch, rows, [17.05, 17.05, None],
                              dump_dir=str(tmp_path))
    out = c.confirm(SPEC, is_submitted=lambda: True,
                    deadline=_time.time() + 0.4, baseline=())
    assert calls, "应走价格列 ROI 识别"
    # 首次点击 + 复核仍读作『发送』→ 补点一次（改动 2）
    assert out.ok is True and out.clicks == 2
    assert c._price_probe is not None
    assert c._price_probe["adopted"] == 17.05
    assert c._price_probe["accepted"] is True
    assert c._price_probe["band"] is not None


def test_confirm_rejects_roi_price_outside_deviation(monkeypatch, tmp_path):
    """采纳价偏离委托价 >2% → 判不可用，不点击。"""
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 220, price="·83"))
    c, _ = _roi_confirmer(monkeypatch, rows, [20.00, 20.00, 20.00],
                          dump_dir=str(tmp_path))
    out = c.confirm(SPEC, is_submitted=lambda: False,
                    deadline=_time.time() + 0.2, baseline=())
    assert out.ok is False and out.clicks == 0
    assert c._price_probe["adopted"] == 20.0
    assert c._price_probe["accepted"] is False
    assert "价格" in out.detail


def test_confirm_rejects_roi_price_outside_daily_limits(
        monkeypatch, tmp_path):
    """采纳价在偏离范围内但越过当日涨停 → 判不可用，不点击。"""
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 220, price="·83"))
    spec = SignalSpec(code6="002056", direction="sell", volume=300,
                      price=17.05, sent_at=datetime(2026, 11, 4, 14, 52, 42),
                      limit_up=17.10, limit_down=17.00)
    c, _ = _roi_confirmer(monkeypatch, rows, [17.30, 17.30, 17.30],
                          dump_dir=str(tmp_path))
    out = c.confirm(spec, is_submitted=lambda: False,
                    deadline=_time.time() + 0.2, baseline=())
    assert out.ok is False and out.clicks == 0
    assert c._price_probe["adopted"] == 17.3
    assert c._price_probe["accepted"] is False


def test_price_roi_skipped_when_multiple_candidates(monkeypatch):
    """同代码/方向/数量/时间窗出现两行 → 不投票、不回填（不许猜）。"""
    rows = _rows(
        _row_words("0020S6", "300", "14", "52", "42", 220, price="·83"),
        _row_words("0020S6", "300", "14", "52", "40", 320, price="·83"),
    )
    c = TdxSignalConfirmer(timeout=0.2)
    calls = []
    monkeypatch.setattr(c, "_ocr_price_roi",
                        lambda *a, **k: calls.append(a) or 17.05)
    monkeypatch.setattr(c, "_grab", lambda _w: None)
    hit, conflict = c._recover_hit_via_price_roi(
        WindowRef(100, "交易信号", (0, 0, 800, 500), True, True),
        rows, SPEC, ())
    assert hit is None
    assert conflict is False
    assert calls == []
    assert c._price_probe["candidates"] == 2
    assert c._price_probe["accepted"] is False


# ------------------------------------------------ 2026-11-18 点击未生效收口


def _win():
    return WindowRef(100, "交易信号", (0, 0, 800, 500), True, True)


def test_wait_frame_ready_needs_two_stable_frames(monkeypatch):
    """画面就绪 = 行非空 + 价格列区间可推 + 连续两帧行指纹一致。"""
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 220))
    c = TdxSignalConfirmer(timeout=0.2)
    seq = iter([[], rows, rows])
    monkeypatch.setattr(c, "_read_rows", lambda _w, **_kw: next(seq))
    monkeypatch.setattr(mod, "_READY_TIMEOUT", 0.5)
    monkeypatch.setattr(mod, "_READY_INTERVAL", 0.0)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    assert c._wait_frame_ready(_win()) is True


def test_wait_frame_ready_times_out_without_price_band(monkeypatch):
    """价格列整体缺失（强制显示后仍在重绘）→ 超上限返回 False，不抛异常。"""
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 220,
                            price="市价"))
    c = TdxSignalConfirmer(timeout=0.2)
    monkeypatch.setattr(c, "_read_rows", lambda _w, **_kw: rows)
    monkeypatch.setattr(mod, "_READY_TIMEOUT", 0.02)
    monkeypatch.setattr(mod, "_READY_INTERVAL", 0.0)
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)
    assert c._wait_frame_ready(_win()) is False


def test_confirm_reclicks_once_when_verify_false(monkeypatch, tmp_path):
    """点击后复核仍读作『发送』→ 重取帧补点一次，第二次复核通过即成功。"""
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 220))
    c, clicks = _fake_confirmer(monkeypatch, rows, dump_dir=str(tmp_path))
    states = iter([False, True])
    monkeypatch.setattr(c, "_verify_clicked_row",
                        lambda _w, _h: next(states))
    out = c.confirm(SPEC, is_submitted=lambda: False,
                    deadline=_time.time() + 1.0, baseline=())
    assert out.ok is True
    assert len(clicks) == 2 and out.clicks == 2


def test_confirm_reclick_stops_after_one_retry(monkeypatch, tmp_path):
    """补点上限 1 次：仍复核 False → 不再点第三次，走既有失败路径。"""
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 220))
    c, clicks = _fake_confirmer(monkeypatch, rows, dump_dir=str(tmp_path))
    monkeypatch.setattr(c, "_verify_clicked_row", lambda _w, _h: False)
    out = c.confirm(SPEC, is_submitted=lambda: False,
                    deadline=_time.time() + 0.3, baseline=())
    assert out.ok is False
    assert len(clicks) == 2


def test_confirm_no_reclick_when_verify_none(monkeypatch, tmp_path):
    """_verify_clicked_row 返回 None 的语义不变：不补点，继续等委托表。"""
    rows = _rows(_row_words("0020S6", "300", "14", "52", "42", 220))
    c, clicks = _fake_confirmer(monkeypatch, rows, dump_dir=str(tmp_path))
    monkeypatch.setattr(c, "_verify_clicked_row", lambda _w, _h: None)
    out = c.confirm(SPEC, is_submitted=lambda: False,
                    deadline=_time.time() + 0.3, baseline=())
    assert out.ok is False
    assert len(clicks) == 1


def test_pending_row_state_three_states(monkeypatch):
    """仍在待发送/已发送/不可判 三态（引擎据此决定重排还是保持 expire）。"""
    words = _row_words("0020S6", "300", "14", "52", "42", 220)
    c = TdxSignalConfirmer(timeout=0.2)
    monkeypatch.setattr(c, "_pick_window", _win)
    monkeypatch.setattr(c, "_read_rows", lambda _w, **_kw: _rows(words))
    assert c.pending_row_state(SPEC) == "pending"

    sent_words = [w for w in words if w[0] not in ("发", "送")]
    sent_words.append(("已发送", 677, 220, 39, 11))
    monkeypatch.setattr(c, "_read_rows",
                        lambda _w, **_kw: _rows(sent_words))
    assert c.pending_row_state(SPEC) == "absent"

    monkeypatch.setattr(c, "_read_rows", lambda _w, **_kw: [])
    assert c.pending_row_state(SPEC) == "unknown"
    monkeypatch.setattr(c, "_pick_window", lambda: None)
    assert c.pending_row_state(SPEC) == "unknown"



