"""F4：多数据源/多周期口径抽样交叉校验（默认关闭，只观测）。

同一交易日的收盘价可由两路独立口径得到：
- 通达信日线（``TQData.get_history_raw``，period=1d）；
- 实盘行情路径：tdx/ths/预警/掘金会话用快照合成的最后一根 1m 收盘价，
  iquant/其他桥会话用桥 ``/quote`` 的当日 1d bar。

两者本应一致（同一交易所同一收盘价）。相差超过阈值通常意味着：取数源落盘
滞后、复权口径不一致、快照合成丢分钟、或桥/终端返回了错误标的——都属于
"数据错了但交易照跑"的高风险静默故障。收盘后抽样若干代码比较，偏差超阈值
记 ``gate=data_cross_check`` 的 risk 事件（只观测，不参与任何交易判定）。

本模块只放纯函数（抽样/偏差计算/报告组装），引擎侧负责两路取数与事件落库。
"""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional


def pick_sample(codes: Iterable[str], size: int) -> List[str]:
    """从股票池按固定步长抽样（确定性，便于复盘；不足 size 时全取）。"""
    ordered = sorted({str(c) for c in (codes or []) if c})
    if size <= 0 or not ordered:
        return []
    if len(ordered) <= size:
        return ordered
    step = max(1, len(ordered) // size)
    return ordered[::step][:size]


def pct_deviation(a: float, b: float) -> float:
    """两价相对偏差（%，以 b 为基准）；b 非正 → inf（无法比较，必然告警）。"""
    try:
        a = float(a)
        b = float(b)
    except (TypeError, ValueError):
        return float("inf")
    if b <= 0 or a <= 0:
        return float("inf")
    return abs(a - b) / b * 100.0


def compare_closes(
    primary: Dict[str, float],
    secondary: Dict[str, float],
    max_dev_pct: float,
) -> dict:
    """比较两路收盘价，返回结构化报告。

    - 只比较两路都有有效正价的代码（单边缺失记入 ``missing``，不告警）；
    - 偏差 > ``max_dev_pct`` 记入 ``deviations``；
    - 返回 ``{"checked", "missing", "deviations", "max_dev_pct", "max_seen_pct"}``。
    """
    primary = primary or {}
    secondary = secondary or {}
    checked = 0
    missing: List[str] = []
    deviations: List[dict] = []
    max_seen = 0.0
    for code in sorted(set(primary) | set(secondary)):
        a = primary.get(code)
        b = secondary.get(code)
        if a is None or b is None:
            missing.append(code)
            continue
        dev = pct_deviation(a, b)
        checked += 1
        if dev != float("inf"):
            max_seen = max(max_seen, dev)
        if dev > float(max_dev_pct):
            deviations.append({
                "code": code,
                "primary": float(a),
                "secondary": float(b),
                "dev_pct": None if dev == float("inf") else round(dev, 4),
            })
    return {
        "checked": checked,
        "missing": missing,
        "deviations": deviations,
        "max_dev_pct": float(max_dev_pct),
        "max_seen_pct": None if max_seen == float("inf") else round(max_seen, 4),
    }


def format_report(report: dict, *, primary_name: str = "tq_1d",
                  secondary_name: str = "live_quote") -> str:
    """报告 → 一行可读文本（事件 message / 通知正文）。"""
    devs = report.get("deviations") or []
    head = "%s vs %s 交叉校验：%d 只可比" % (
        primary_name, secondary_name, int(report.get("checked") or 0))
    if not devs:
        return head + "，无超阈值偏差"
    shown = devs[:10]
    parts = []
    for d in shown:
        dev = "inf" if d.get("dev_pct") is None else "%.3f%%" % d["dev_pct"]
        parts.append("%s(%s→%s, %s)" % (
            d.get("code"), d.get("primary"), d.get("secondary"), dev))
    text = head + "，%d 只偏差超 %.3f%%：%s" % (
        len(devs), float(report.get("max_dev_pct") or 0), "、".join(parts))
    if len(devs) > len(shown):
        text += " 等"
    return text


def positive_price(value) -> Optional[float]:
    """行情价 → 正 float；None/非数字/<=0 → None（停牌/伪影不参与比较）。"""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return num if num > 0 else None
