# -*- coding: utf-8 -*-
"""rating_core 口径回归：最小根数、ENH 方向、空头偏离分、超卖对称性。"""
import numpy as np
import pandas as pd
import polars as pl

from core.engine.rating_core import MIN_BARS, compute_rating_frame


def _frame(closes) -> pl.DataFrame:
    closes = np.asarray(closes, dtype=float)
    n = len(closes)
    dates = pd.bdate_range("2020-01-01", periods=n)
    return pl.DataFrame({
        "datetime": [d.to_pydatetime() for d in dates],
        "Open": closes,
        "High": closes * 1.005,
        "Low": closes * 0.995,
        "Close": closes,
        "Volume": np.full(n, 1_000_000.0),
    })


def _random_closes(n: int, seed: int = 7, drift: float = 0.0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return 100 * np.cumprod(1 + rng.normal(drift, 0.012, n))


def test_min_bars_blocks_direction():
    df = _frame(_random_closes(MIN_BARS - 1))
    assert compute_rating_frame(df).empty
    # 门控侧显式 min_bars=0 时仍逐日输出（保持 fail-open 行为）
    assert len(compute_rating_frame(df, min_bars=0)) == MIN_BARS - 1


def test_oversold_requires_negative_bias():
    out = compute_rating_frame(_frame(_random_closes(600)))
    assert len(out) == 600
    sold = out[out["oversold"]]
    assert not sold.empty
    # §13：对称 z 阈值，超卖样本的乖离率必须为负（旧 ABS 定义会误伤正乖离）
    assert (sold["bias"] < 0).all()
    bought = out[out["overbought"]]
    assert (bought["bias"] > 0).all()


def test_bear_strength_includes_deviation_score():
    # 单调下跌：价格 20 + 角度 20 + 偏离分_空 10 + 量能 5 = 55
    # （平滑下行时 AO<BO，均线条件 0；若缺偏离分_空 则为 45）
    closes = np.linspace(100, 10, 420)
    out = compute_rating_frame(_frame(closes))
    last = out.iloc[-1]
    assert last["direction"] == -1
    assert last["bias"] < -3
    assert last["dn_str"] >= 55
    assert last["bull_dur"] == 0
    assert last["bear_dur"] > 0


def test_bull_duration_matches_trailing_streak():
    out = compute_rating_frame(_frame(_random_closes(500)))
    flags = out["up_enh"].tolist()
    trailing = 0
    for flag in reversed(flags):
        if not flag:
            break
        trailing += 1
    assert int(out.iloc[-1]["bull_dur"]) == trailing
