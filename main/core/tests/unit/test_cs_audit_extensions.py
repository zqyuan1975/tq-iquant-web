"""2026-11-23 收尾批次：P1-1/P1-5/P1-6/P1-7 后端诊断与口径单测。"""
import json
from datetime import date, datetime

import polars as pl

from core.services import backtest_service as svc
from core.services.backtest_export import _as_pct, build_csv
from core.services.html_report import _gate_val


# ---------------- P1-1：逐根日期对齐 ----------------

def _bars(*stamps):
    return [datetime(2026, 7, 29, h, m) for h, m in stamps]


def test_align_formula_bars_ok():
    data = {"X": [{"Date": "20260729", "Value": 1.0},
                  {"Date": "20260729", "Value": 2.0}]}
    assert svc.align_formula_bars(
        data, _bars((10, 0), (10, 30)),
        code="000001.SZ", strategy_id=1) is None


def test_align_formula_bars_flags_bar_count_mismatch():
    data = {"X": [{"Date": "20260729", "Value": 1.0}] * 3}
    diag = svc.align_formula_bars(
        data, _bars((10, 0), (10, 30)),
        code="000001.SZ", strategy_id=1)

    assert diag["problem_count"] == 1
    assert diag["problems"][0] == {
        "day": "2026-07-29", "kind": "bar_count_mismatch",
        "formula": 3, "bars": 2,
    }


def test_align_formula_bars_flags_unknown_date():
    data = {"X": [{"Date": "20260730", "Value": 1.0}]}
    diag = svc.align_formula_bars(
        data, _bars((10, 0)), code="000001.SZ", strategy_id=1)

    assert diag["problems"][0]["kind"] == "date_not_in_klines"
    assert diag["problems"][0]["formula"] == 1


def test_align_formula_bars_dedupes_variables_and_skips_meta():
    data = {
        "X": [{"Date": "20260729", "Value": 1.0}] * 2,
        "Y": [{"Date": "20260729", "Value": 1.0}] * 2,
        "ErrorId": "0",
    }
    assert svc.align_formula_bars(
        data, _bars((10, 0), (10, 30)),
        code="000001.SZ", strategy_id=1) is None


def test_align_formula_bars_none_without_bar_times():
    """日线（无分钟时间轴）不做逐根比对，避免误报。"""
    assert svc.align_formula_bars(
        {"X": [{"Date": "20260729", "Value": 1.0}]}, None,
        code="000001.SZ", strategy_id=1) is None


# ---------------- P1-5：数据指纹 ----------------

def _klines(rows):
    return {code: {"1d": pl.DataFrame({"datetime": ts})}
            for code, ts in rows.items()}


def test_build_data_fingerprint_per_code_and_stable():
    kl = _klines({"A": [datetime(2026, 7, 29), datetime(2026, 7, 30)],
                  "B": [datetime(2026, 7, 29)]})
    fp1 = svc.build_data_fingerprint(kl, date(2026, 7, 29), date(2026, 7, 30))
    fp2 = svc.build_data_fingerprint(kl, date(2026, 7, 29), date(2026, 7, 30))

    assert fp1["version"] == svc.DATA_FINGERPRINT_VERSION
    assert fp1["codes"] == 2
    assert fp1["per_code"]["A"]["1d"] == {
        "bars": 2, "first": "2026-07-29", "last": "2026-07-30"}
    assert fp1["per_code"]["B"]["1d"]["bars"] == 1
    assert fp1["hash"] == fp2["hash"]


def test_build_data_fingerprint_changes_when_data_changes():
    base = _klines({"A": [datetime(2026, 7, 29)]})
    more = _klines({"A": [datetime(2026, 7, 29), datetime(2026, 7, 30)]})
    h1 = svc.build_data_fingerprint(base, date(2026, 7, 29),
                                    date(2026, 7, 30))["hash"]
    h2 = svc.build_data_fingerprint(more, date(2026, 7, 29),
                                    date(2026, 7, 30))["hash"]
    assert h1 != h2


def test_attach_data_fingerprint_merges_snapshot():
    fp = {"version": "v1", "hash": "abc", "per_code": {}}

    class _R:
        params_snapshot = json.dumps({"dividend_mode": "exact"})

    rec = _R()
    svc.attach_data_fingerprint(rec, fp)
    payload = json.loads(rec.params_snapshot)
    assert payload["dividend_mode"] == "exact"
    assert payload["data"]["hash"] == "abc"


# ---------------- P1-7：空日三分类 + 上市日推导 ----------------

def test_empty_day_classification_and_listing_inferred():
    # 2026-07-26 周日；7/28 有数据；7/27 为交易日但全池无 bar（最后数据日之前
    # → all_halted）；7/29–7/31 为交易日无 bar，在最后数据日之后 → not_published。
    kl = _klines({"A": [datetime(2026, 7, 28)]})
    dq = svc.summarize_data_quality({}, kl, ["A"], date(2026, 7, 26),
                                    date(2026, 7, 31))
    cls = dq["periods"]["1d"]["empty_day_classification"]

    assert cls["non_trading_days"] == 1      # 7/26 周日
    assert cls["all_halted_days"] == 1       # 7/27 在最后数据日(7/28)之前
    assert cls["not_published_days"] == 3    # 7/29、7/30、7/31 在最后数据日之后
    assert dq["listing_inferred"]["source"] == "first_bar"
    assert dq["listing_inferred"]["dates"] == {"A": "2026-07-28"}


def test_listing_inferred_marks_mid_window_first_bar():
    kl = _klines({"A": [datetime(2026, 7, 29), datetime(2026, 7, 30)]})
    dq = svc.summarize_data_quality({}, kl, ["A"], date(2026, 7, 27),
                                    date(2026, 7, 30))
    assert dq["listing_inferred"]["dates"]["A"] == "2026-07-29"
    # 仅诊断：不裁剪、不影响 kept_bars 统计口径（原有字段照旧）
    assert dq["periods"]["1d"]["kept_bars"] == 2


# ---------------- P1-6：单位契约 ----------------

def test_as_pct_converts_decimals_and_keeps_blanks():
    assert _as_pct(0.1234) == 12.34
    assert _as_pct(-0.05) == -5.0
    assert _as_pct(None) == ""
    assert _as_pct("") == ""
    assert _as_pct("n/a") == "n/a"


def test_csv_metrics_headers_carry_percent_suffix():
    detail = {
        "record": {"id": 1, "name": "r", "start_date": "2026-01-01",
                   "end_date": "2026-02-01"},
        "evaluations": {"total_return": 0.2, "annual_return": 0.1,
                        "max_drawdown": -0.05, "sharpe_ratio": 1.5,
                        "win_rate": 0.6, "total_trades": 10},
        "cost_capacity": {"explicit": {"total_cost": 12.0,
                                       "cost_drag_ratio": 0.01}},
    }
    csv_text = build_csv(detail, "metrics")
    lines = csv_text.strip().splitlines()

    assert ("总收益(%)", "20.0") in [tuple(x.split(",")) for x in lines]
    assert ("年化收益(%)", "10.0") in [tuple(x.split(",")) for x in lines]
    assert ("最大回撤(%)", "-5.0") in [tuple(x.split(",")) for x in lines]
    assert ("胜率(%)", "60.0") in [tuple(x.split(",")) for x in lines]
    assert "夏普比率,1.5" in lines


def test_csv_snapshots_headers_and_values_percent():
    detail = {"snapshots": [{
        "snap_date": "2026-01-05", "total_value": 100, "cash": 10,
        "market_value": 90, "daily_return": 0.01,
        "cumulative_return": 0.05, "benchmark_value": 100,
        "gate_state": "on", "gate_strength": 0.8,
    }]}
    lines = build_csv(detail, "snapshots").strip().splitlines()

    # 金额类列统一带 (元)；收益率列带 (%)；门控列不带单位
    assert lines[0].lstrip("\ufeff") == (
        "日期,总资产(元),现金(元),持仓市值(元),日收益(%),"
        "累计收益(%),基准值(元),门控状态,门控强度")
    assert lines[1].split(",")[4] == "1.0"
    assert lines[1].split(",")[5] == "5.0"


def test_gate_val_uses_fixed_unit_no_magnitude_guessing():
    """0.5 不再被猜成 50%，统一按原始数值 4 位小数。"""
    assert _gate_val(0.5) == "0.5000"
    assert _gate_val(3) == "3.0000"
    assert _gate_val(None) == "—"
    assert _gate_val("abc") == "abc"
