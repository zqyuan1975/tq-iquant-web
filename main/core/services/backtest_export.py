"""K6 回测报告导出：CSV（成交 / 日快照 / 指标）与 PDF（HTML 报告打印）。

设计取舍：
- CSV 直接用标准库 csv + UTF-8 BOM（Excel 打开不乱码），零依赖；
- PDF 不引入 reportlab/weasyprint（离线装不上），改为调用系统已有的
  Chromium 内核浏览器（Edge/Chrome）无头打印 HTML 报告——中文字体、
  图表、样式与在线报告完全一致。找不到浏览器时抛 PdfExportUnavailable，
  路由层转 503 并提示可用 CSV/HTML 替代。
"""

import csv
import io
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

CSV_KINDS = ("trades", "snapshots", "metrics")

_TRADE_TYPE_LABEL = {"BUY": "买入", "SELL": "卖出"}


def _as_pct(v):
    """内部小数 → 导出百分数（保留 4 位）；空值保持为空，非数值原样返回。"""
    if v is None or v == "":
        return ""
    try:
        return round(float(v) * 100, 4)
    except (TypeError, ValueError):
        return v


_BROWSER_CANDIDATES = (
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
)


class PdfExportUnavailable(RuntimeError):
    """离线环境找不到可用的 Chromium 内核浏览器（Edge/Chrome）。"""


def build_csv(detail: dict, kind: str) -> str:
    """详情 payload → CSV 文本（含 UTF-8 BOM，Excel 直接可读）。

    kind ∈ trades（成交明细）/ snapshots（组合日快照）/ metrics（指标汇总）。
    数值原样输出（不格式化千分位），便于二次计算。
    """
    if kind not in CSV_KINDS:
        raise ValueError(f"不支持的导出类型：{kind}")
    buf = io.StringIO()
    writer = csv.writer(buf)

    if kind == "trades":
        writer.writerow(["时间", "策略", "信号", "代码", "方向", "价格", "数量",
                         "金额(元)", "佣金(元)", "印花税(元)", "过户费(元)"])
        for t in detail.get("trades") or []:
            writer.writerow([
                t.get("bar_time") or "",
                t.get("strategy_name") or t.get("strategy_id") or "",
                t.get("signal_name") or "",
                t.get("stock_code") or "",
                _TRADE_TYPE_LABEL.get(t.get("trade_type"), t.get("trade_type") or ""),
                t.get("price"), t.get("quantity"), t.get("amount"),
                t.get("commission"), t.get("stamp_duty"), t.get("transfer_fee"),
            ])
    elif kind == "snapshots":
        # P1-6（2026-11-23）：内部口径统一小数；导出统一百分数，字段名带 (%)。
        writer.writerow(["日期", "总资产(元)", "现金(元)", "持仓市值(元)", "日收益(%)",
                         "累计收益(%)", "基准值(元)", "门控状态", "门控强度"])
        for s in detail.get("snapshots") or []:
            writer.writerow([
                s.get("snap_date") or "",
                s.get("total_value"), s.get("cash"), s.get("market_value"),
                _as_pct(s.get("daily_return")),
                _as_pct(s.get("cumulative_return")),
                s.get("benchmark_value"),
                s.get("gate_state") or "", s.get("gate_strength"),
            ])
    else:  # metrics
        ev = detail.get("evaluations") or {}
        rec = detail.get("record") or {}
        cc = detail.get("cost_capacity") or {}
        ex = cc.get("explicit") or {}
        rows = [
            ("记录 ID", rec.get("id")),
            ("记录名称", rec.get("name")),
            ("起止日期", f"{rec.get('start_date') or ''} ~ {rec.get('end_date') or ''}"),
            ("总收益(%)", _as_pct(ev.get("total_return"))),
            ("年化收益(%)", _as_pct(ev.get("annual_return"))),
            ("最大回撤(%)", _as_pct(ev.get("max_drawdown"))),
            ("年化波动率(%)", _as_pct(ev.get("volatility"))),
            ("夏普比率", ev.get("sharpe_ratio")),
            ("卡玛比率", ev.get("calmar_ratio")),
            ("胜率(%)", _as_pct(ev.get("win_rate"))),
            ("盈亏比", ev.get("profit_factor")),
            ("成交笔数", ev.get("total_trades")),
            ("基准收益(%)", _as_pct(ev.get("benchmark_return"))),
            ("年化超额(%)", _as_pct(ev.get("excess_annual_return"))),
            ("年化换手（双边，%）", _as_pct(ev.get("annual_turnover"))),
            ("交易成本/日均净值(%)", _as_pct(ev.get("total_cost_ratio"))),
            ("交易成本合计(元)", ex.get("total_cost")),
            ("成本拖累占比(%)", _as_pct(ex.get("cost_drag_ratio"))),
        ]
        writer.writerow(["指标", "数值"])
        for label, value in rows:
            writer.writerow([label, "" if value is None else value])

    return "\ufeff" + buf.getvalue()


def find_pdf_browser() -> Optional[str]:
    """定位可用浏览器：环境变量 TQ_PDF_BROWSER 优先，其次常见安装路径/PATH。"""
    env = os.environ.get("TQ_PDF_BROWSER")
    if env and Path(env).is_file():
        return env
    for path in _BROWSER_CANDIDATES:
        if Path(path).is_file():
            return path
    for name in ("msedge.exe", "chrome.exe"):
        found = shutil.which(name)
        if found:
            return found
    return None


def html_to_pdf(html: str, timeout: int = 120) -> bytes:
    """HTML → PDF（无头浏览器打印）。找不到浏览器或打印失败抛 PdfExportUnavailable。"""
    exe = find_pdf_browser()
    if exe is None:
        raise PdfExportUnavailable(
            "未找到 Microsoft Edge / Google Chrome，无法生成 PDF；"
            "可设置环境变量 TQ_PDF_BROWSER 指向浏览器可执行文件，"
            "或改用 CSV / HTML 导出")
    with tempfile.TemporaryDirectory(prefix="bt-pdf-") as td:
        html_path = Path(td) / "report.html"
        pdf_path = Path(td) / "report.pdf"
        html_path.write_text(html, encoding="utf-8")
        # 新版 Chromium 用 --headless=new；旧版 Edge 仅认 --headless，两次尝试
        for headless in ("--headless=new", "--headless"):
            cmd = [
                exe, headless, "--disable-gpu", "--no-sandbox",
                "--no-pdf-header-footer", "--virtual-time-budget=8000",
                f"--print-to-pdf={pdf_path}", html_path.as_uri(),
            ]
            try:
                subprocess.run(cmd, capture_output=True, timeout=timeout, check=False)
            except (OSError, subprocess.SubprocessError):
                continue
            if pdf_path.is_file() and pdf_path.stat().st_size > 0:
                data = pdf_path.read_bytes()
                if data.startswith(b"%PDF"):
                    return data
                pdf_path.unlink()
    raise PdfExportUnavailable("浏览器未能生成 PDF（可能被安全策略拦截或超时）")
