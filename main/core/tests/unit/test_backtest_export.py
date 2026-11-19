"""K6 导出（CSV / PDF）单测。

CSV 是纯函数：直接喂详情 payload 校验表头/内容/BOM。
PDF 依赖外部浏览器，单测只覆盖控制流：找不到浏览器 → 明确异常；
浏览器产出 %PDF 文件 → 原样返回。
"""

from pathlib import Path
from types import SimpleNamespace

import pytest

from core.services import backtest_export as be


def _detail() -> dict:
    return {
        "record": {
            "id": 7, "name": "导出用例",
            "start_date": "2026-07-29", "end_date": "2026-07-31",
        },
        "trades": [
            {
                "bar_time": "2026-07-30 10:00:00", "strategy_name": "s1",
                "signal_name": "open_sig", "stock_code": "000001.SZ",
                "trade_type": "BUY", "price": 10.0, "quantity": 1000,
                "amount": 10000.0, "commission": 5.0,
                "stamp_duty": 0.0, "transfer_fee": 0.0,
            },
            {
                "bar_time": "2026-07-31 14:00:00", "strategy_name": "s1",
                "signal_name": "close_sig", "stock_code": "000001.SZ",
                "trade_type": "SELL", "price": 11.0, "quantity": 1000,
                "amount": 11000.0, "commission": 5.0,
                "stamp_duty": 5.5, "transfer_fee": 0.1,
            },
        ],
        "snapshots": [
            {"snap_date": "2026-07-30", "total_value": 100000.0, "cash": 90000.0,
             "market_value": 10000.0, "daily_return": 0.0,
             "cumulative_return": 0.0, "benchmark_value": 1.0,
             "gate_state": "neutral", "gate_strength": 0.5},
        ],
        "evaluations": {
            "total_return": 0.01, "annual_return": 0.12, "max_drawdown": -0.05,
            "total_trades": 2,
        },
        "cost_capacity": {
            "explicit": {"total_cost": 10.6, "cost_drag_ratio": 0.02},
        },
    }


def test_build_csv_trades():
    text = be.build_csv(_detail(), "trades")
    assert text.startswith("\ufeff")           # Excel 友好 BOM
    lines = text.lstrip("\ufeff").splitlines()
    assert lines[0].split(",")[:5] == ["时间", "策略", "信号", "代码", "方向"]
    assert "000001.SZ" in lines[1]
    assert "买入" in lines[1]
    assert "卖出" in lines[2]
    sell = lines[2].split(",")
    assert sell[4] == "卖出"
    assert sell[8:11] == ["5.0", "5.5", "0.1"]  # 佣金 / 印花税 / 过户费


def test_build_csv_snapshots_and_metrics():
    snap = be.build_csv(_detail(), "snapshots").lstrip("\ufeff").splitlines()
    assert snap[0].split(",")[0] == "日期"
    assert snap[1].startswith("2026-07-30")

    metrics = be.build_csv(_detail(), "metrics").lstrip("\ufeff").splitlines()
    assert metrics[0] == "指标,数值"
    joined = "\n".join(metrics)
    assert "总收益" in joined and "0.01" in joined
    assert "交易成本合计" in joined and "10.6" in joined


def test_build_csv_rejects_unknown_kind():
    with pytest.raises(ValueError):
        be.build_csv(_detail(), "pdf")


def test_find_pdf_browser_prefers_env(tmp_path, monkeypatch):
    fake = tmp_path / "browser.exe"
    fake.write_bytes(b"stub")
    monkeypatch.setenv("TQ_PDF_BROWSER", str(fake))
    assert be.find_pdf_browser() == str(fake)


def test_html_to_pdf_unavailable(monkeypatch):
    monkeypatch.delenv("TQ_PDF_BROWSER", raising=False)
    monkeypatch.setattr(be, "find_pdf_browser", lambda: None)
    with pytest.raises(be.PdfExportUnavailable):
        be.html_to_pdf("<html></html>")


def test_html_to_pdf_success_control_flow(tmp_path, monkeypatch):
    """浏览器写入 %PDF 文件 → 原样返回字节；不依赖真实浏览器。"""
    monkeypatch.setattr(be, "find_pdf_browser", lambda: "C:/fake/msedge.exe")

    def fake_run(cmd, **kwargs):
        target = next(c for c in cmd if c.startswith("--print-to-pdf="))
        Path(target.split("=", 1)[1]).write_bytes(b"%PDF-1.4 fake")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(be.subprocess, "run", fake_run)
    assert be.html_to_pdf("<html></html>").startswith(b"%PDF")


def test_html_to_pdf_gives_up_without_output(monkeypatch):
    monkeypatch.setattr(be, "find_pdf_browser", lambda: "C:/fake/msedge.exe")
    monkeypatch.setattr(
        be.subprocess, "run", lambda cmd, **kw: SimpleNamespace(returncode=0))
    with pytest.raises(be.PdfExportUnavailable):
        be.html_to_pdf("<html></html>")
