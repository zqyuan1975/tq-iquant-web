# -*- coding: utf-8 -*-
"""资金池基线批跑器（0021 E6）。

对同一组组合与同一段行情，按四种资金口径各跑一遍，产出可直接对比的
基线报告，用于解释「动态会话池」相对静态方案的增益来源：

  * ``standalone``：各组合按自身初始资金独立运行（不共享、不借贷）；
  * ``static_equal``：把组合资金合计按成员数等额切分后各自独立运行；
  * ``static_ratio``：按调用方给定权重切分合计资金后各自独立运行；
  * ``dynamic_pool``：保留各组合自有资金，走 ``run_multi`` + E2 会话池
    动态额度（成交瞬间拆借，Σ 现金守恒）。

设计约束：
  - 纯批跑与汇总，不落库、不改配置、不触实盘；
  - 每个基线都从调用方提供的工厂获取**全新** Portfolio，避免持仓/现金
    在多次运行间串味；
  - 参数与结果均做确定性序列化，同一输入必得同一 ``params_hash`` 与
    同一报告，可复算。
"""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Callable, Dict, List, Optional, Sequence

BASELINE_VERSION = "pairing-baseline-e6"
BASELINE_KINDS = ("standalone", "static_equal", "static_ratio", "dynamic_pool")

DEFAULT_POOL_PLAN = {
    "enabled": True,
    "min_share": "0.05",
    "max_share": "1.0",
    "min_cash_reserve_pct": "0.03",
}


class BaselineError(ValueError):
    """基线批跑入参不合法。"""


def _s(value) -> str:
    """Decimal 稳定字符串（去尾随零，整数不带小数点）。"""
    if value is None:
        return None
    dec = Decimal(str(value))
    text = format(dec.normalize(), "f")
    return text


def params_hash(payload: dict) -> str:
    """参数哈希：键排序 + 紧凑分隔符，跨平台稳定。"""
    canonical = json.dumps(
        payload, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _initial_capital(portfolio) -> Decimal:
    cap = getattr(portfolio, "account", None)
    value = getattr(cap, "initial_capital", None)
    if value is None:
        value = getattr(portfolio, "initial_capital", 0)
    return Decimal(str(value or 0))


def _set_capital(portfolio, capital: Decimal) -> None:
    """把组合账面资金改成指定额度（静态基线用）。"""
    account = getattr(portfolio, "account", None)
    if account is None:
        raise BaselineError("组合缺少 account，无法设置基线资金")
    account.initial_capital = capital
    account.cash = capital


def _final_equity(result: dict, fallback: Decimal) -> Decimal:
    for snap in reversed(result.get("snapshots") or []):
        value = snap.get("total_value")
        if value is not None:
            return Decimal(str(value))
    return fallback


def _member_metrics(portfolio_id: int, capital: Decimal, result: dict) -> dict:
    equity = _final_equity(result, capital)
    total_return = (equity / capital - Decimal("1")) if capital else None
    return {
        "portfolio_id": portfolio_id,
        "capital": _s(capital),
        "final_equity": _s(equity),
        "total_return": _s(total_return),
        "trades": len(result.get("trades") or []),
    }


def _summarize(kind: str, members: List[dict]) -> dict:
    total_capital = sum(Decimal(m["capital"]) for m in members)
    total_equity = sum(Decimal(m["final_equity"]) for m in members)
    total_return = (
        total_equity / total_capital - Decimal("1")
        if total_capital else None)
    return {
        "kind": kind,
        "total_capital": _s(total_capital),
        "total_equity": _s(total_equity),
        "total_return": _s(total_return),
        "members": members,
    }


def _run_independent(engine, portfolios, klines, signal_cache,
                     benchmark_data, run_options) -> List[dict]:
    members = []
    for portfolio in portfolios:
        capital = _initial_capital(portfolio)
        result = engine.run(
            portfolio, klines=klines, signal_cache=signal_cache,
            benchmark_data=benchmark_data, run_options=run_options)
        members.append(_member_metrics(portfolio.portfolio_id, capital, result))
    return members


def _split_capital(portfolios, weights: Optional[Dict[int, Decimal]]):
    """按等额或权重把合计资金切分到每个组合，返回 [(portfolio, capital)]。"""
    caps = [_initial_capital(p) for p in portfolios]
    total = sum(caps)
    if weights is None:
        share = total / Decimal(len(portfolios))
        return [(p, share) for p in portfolios]
    normalized = {
        int(pid): Decimal(str(weight)) for pid, weight in weights.items()
    }
    weight_sum = sum(normalized.values())
    if weight_sum <= 0:
        raise BaselineError("static_ratio 权重合计必须大于 0")
    missing = [
        p.portfolio_id for p in portfolios
        if p.portfolio_id not in normalized
    ]
    if missing:
        raise BaselineError("static_ratio 缺少组合权重：%s" % missing)
    return [
        (p, total * normalized[p.portfolio_id] / weight_sum)
        for p in portfolios
    ]


def run_baselines(
    portfolio_factory: Callable[[], Sequence],
    klines: dict,
    signal_cache: Optional[dict] = None,
    benchmark_data: Optional[dict] = None,
    run_options: Optional[dict] = None,
    weights: Optional[Dict[int, Decimal]] = None,
    pool_plan: Optional[dict] = None,
    engine=None,
    kinds: Sequence[str] = BASELINE_KINDS,
) -> dict:
    """跑四种基线并返回可复算报告。

    ``portfolio_factory`` 每次调用必须返回**全新**组合对象；同一对象不能在
    不同基线间复用（回测会改变现金与持仓）。
    """
    kinds = tuple(kinds)
    unknown = [k for k in kinds if k not in BASELINE_KINDS]
    if unknown:
        raise BaselineError("未知基线类型：%s" % unknown)
    if not kinds:
        raise BaselineError("至少选择一种基线")

    probe = list(portfolio_factory())
    if not probe:
        raise BaselineError("组合列表为空")
    portfolio_ids = [p.portfolio_id for p in probe]
    if len(set(portfolio_ids)) != len(portfolio_ids):
        raise BaselineError("组合 ID 重复：%s" % portfolio_ids)

    resolved_pool = dict(DEFAULT_POOL_PLAN)
    if pool_plan:
        resolved_pool.update(pool_plan)
    resolved_pool["enabled"] = True

    engine = engine
    if engine is None:
        from core.engine.backtest_engine import BacktestEngine
        engine = BacktestEngine()

    signal_cache = signal_cache if signal_cache is not None else {}

    params = {
        "version": BASELINE_VERSION,
        "kinds": list(kinds),
        "portfolios": [
            {"portfolio_id": p.portfolio_id, "initial_capital": _s(_initial_capital(p))}
            for p in probe
        ],
        "weights": (
            {str(k): _s(v) for k, v in sorted(weights.items())}
            if weights else None
        ),
        "pool_plan": resolved_pool,
        "run_options": run_options or {},
    }
    digest = params_hash(params)

    reports = {}
    for kind in kinds:
        portfolios = list(portfolio_factory())
        if kind == "standalone":
            members = _run_independent(
                engine, portfolios, klines, signal_cache,
                benchmark_data, run_options)
        elif kind == "static_equal":
            for portfolio, capital in _split_capital(portfolios, None):
                _set_capital(portfolio, capital)
            members = _run_independent(
                engine, portfolios, klines, signal_cache,
                benchmark_data, run_options)
        elif kind == "static_ratio":
            for portfolio, capital in _split_capital(portfolios, weights):
                _set_capital(portfolio, capital)
            members = _run_independent(
                engine, portfolios, klines, signal_cache,
                benchmark_data, run_options)
        else:  # dynamic_pool
            results = engine.run_multi(
                portfolios, klines=klines, signal_cache=signal_cache,
                benchmark_data=benchmark_data, run_options=run_options,
                capital_pool_plan=resolved_pool)
            members = [
                _member_metrics(
                    p.portfolio_id, _initial_capital(p),
                    results.get(p.portfolio_id) or {})
                for p in portfolios
            ]
        reports[kind] = _summarize(kind, members)

    return {
        "version": BASELINE_VERSION,
        "params_hash": digest,
        "params": params,
        "baselines": reports,
    }


def recompute_totals(report: dict) -> Dict[str, dict]:
    """从成员明细复算每个基线总额，用于校验报告未被手改。"""
    out = {}
    for kind, payload in (report.get("baselines") or {}).items():
        members = payload.get("members") or []
        total_capital = sum(Decimal(m["capital"]) for m in members)
        total_equity = sum(Decimal(m["final_equity"]) for m in members)
        out[kind] = {
            "total_capital": _s(total_capital),
            "total_equity": _s(total_equity),
            "total_return": _s(
                total_equity / total_capital - Decimal("1")
                if total_capital else None),
        }
    return out
