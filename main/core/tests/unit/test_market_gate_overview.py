# -*- coding: utf-8 -*-
"""E5：统一市场状态面板聚合接口单元测试（只读，不改判定）。"""
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import Base, PortfolioStrategy, Strategy
from core.services.market_gate_service import fetch_gate_overview


def _db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _strategy(pid: int, name: str, **kw) -> Strategy:
    return Strategy(portfolio_id=pid, name=name, formula_id=1, period="1d",
                    role="independent", **kw)


def test_overview_aggregates_gate_and_regime_config():
    db = _db()
    p = PortfolioStrategy(
        name="组合A", stock_pool_id=1, benchmark_index="000300.SH",
        market_gate_enabled=True,
        market_gate_indices='["000300.SH", "000905.SH"]',
        market_gate_vote_mode="majority",
        market_gate_strength_threshold=60,
        market_gate_bear_reduce_ratio=Decimal("0.5000"),
    )
    db.add(p)
    db.commit()
    db.add_all([
        _strategy(p.id, "策略1", regime_filter_enabled=True,
                  trend_max_positions=3,
                  trend_trailing_stop_ratio=Decimal("0.0500"),
                  market_gate_mode="off"),
        _strategy(p.id, "策略2", market_gate_mode="on"),
    ])
    db.commit()

    data = fetch_gate_overview(db)

    assert [s["key"] for s in data["scopes"]] == [
        "market_gate", "regime_filter", "precedence"]
    row = data["portfolios"][0]
    assert row["name"] == "组合A"
    assert row["market_gate_enabled"] is True
    assert row["effective_enabled"] is True
    assert row["indices"] == ["000300.SH", "000905.SH"]
    assert row["vote_mode"] == "majority"
    assert row["strength_threshold"] == 60
    assert row["bear_reduce_ratio"] == 0.5
    assert [s["name"] for s in row["strategies"]] == ["策略1", "策略2"]
    assert row["strategies"][0]["regime_filter_enabled"] is True
    assert row["strategies"][0]["trend_max_positions"] == 3
    assert row["strategies"][0]["market_gate_mode"] == "off"
    assert row["strategies"][1]["market_gate_mode"] == "on"


def test_overview_defaults_to_benchmark_and_effective_from_strategy_on():
    db = _db()
    p = PortfolioStrategy(name="组合B", stock_pool_id=1,
                          benchmark_index="000985.CSI")
    db.add(p)
    db.commit()
    db.add(_strategy(p.id, "策略X", market_gate_mode="on"))
    db.commit()

    row = fetch_gate_overview(db)["portfolios"][0]
    # 未配置多指数 → 回落到基准指数（旧语义）；组合开关关但策略 on → 实际启用
    assert row["indices"] == ["000985.CSI"]
    assert row["market_gate_enabled"] is False
    assert row["effective_enabled"] is True
    assert row["vote_mode"] == "any"
    assert row["strength_threshold"] == 0
    assert row["bear_reduce_ratio"] == 0.0
