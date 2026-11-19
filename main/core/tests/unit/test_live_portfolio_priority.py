"""A2 跨组合下单优先级单元测试。"""
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.api.strategies import PortfolioCreate
from core.engine.live_engine import LiveEngine
from core.models import Base, PortfolioStrategy
from core.services import strategy_service


def _engine_with(priorities):
    # 只测排序函数：用轻量对象避免 LiveEngine.__init__ 的依赖
    return SimpleNamespace(portfolios=[
        SimpleNamespace(portfolio_id=i + 1, priority=p)
        for i, p in enumerate(priorities)
    ])


def test_priority_order_desc_and_stable():
    engine = _engine_with([0, 5, 0, 5])
    ordered = LiveEngine._prioritized_portfolios(engine)
    assert [p.portfolio_id for p in ordered] == [2, 4, 1, 3]

    engine = _engine_with([0, 0, 0])
    ordered = LiveEngine._prioritized_portfolios(engine)
    assert [p.portfolio_id for p in ordered] == [1, 2, 3]


def test_priority_invalid_value_falls_back_zero():
    engine = _engine_with(["bad", None, 2])
    ordered = LiveEngine._prioritized_portfolios(engine)
    assert [p.portfolio_id for p in ordered] == [3, 1, 2]


def _req(priority):
    return PortfolioCreate(name="p", stock_pool_id=1, priority=priority)


def test_apply_portfolio_fields_priority_range():
    p = PortfolioStrategy(name="p", initial_capital=100000)
    strategy_service._apply_portfolio_fields(p, _req(7))
    assert p.priority == 7

    with pytest.raises(ValueError):
        strategy_service._apply_portfolio_fields(p, _req(1001))
    with pytest.raises(ValueError):
        strategy_service._apply_portfolio_fields(p, _req("x"))


def test_serialize_portfolio_includes_priority():
    db = sessionmaker(bind=create_engine("sqlite:///:memory:"))()
    Base.metadata.create_all(db.get_bind())
    p = PortfolioStrategy(name="p", stock_pool_id=1, initial_capital=100000,
                          priority=3)
    db.add(p)
    db.commit()
    assert strategy_service.serialize_portfolio(db, p)["priority"] == 3
