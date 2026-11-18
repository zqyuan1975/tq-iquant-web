# -*- coding: utf-8 -*-
"""实盘公司行为接入的轻量验证（J1/J2，2026-11-18）。

不启动 LiveEngine（需要行情/通道），只验证三件结构性问题：
1. 未配置任何组合时 ``_maybe_apply_corporate_actions`` 第一行即返回（不读库、不改账）；
2. 上一交易日回退（事件日落在周末 → 顺延到周一，且 ``after`` 是周五）；
3. 事件集合 digest 稳定且对内容敏感（进 params_snapshot + 增量指纹）。
"""
from datetime import date
from decimal import Decimal

from core.engine.corporate_actions import (
    CorporateActionEvent, CorporateActionTimeline,
)
from core.engine.live_engine import LiveEngine


class _Calendar:
    def is_trading_day(self, day):
        return day.weekday() < 5


def test_no_specs_returns_immediately():
    engine = LiveEngine.__new__(LiveEngine)
    engine._corporate_action_specs = {}
    # 只要第一行生效，就不会去读这些属性（未设置，访问即 AttributeError）
    engine._maybe_apply_corporate_actions(date(2026, 7, 20))


def test_prev_trading_day_skips_weekend():
    engine = LiveEngine.__new__(LiveEngine)
    engine._calendar = _Calendar()
    # 2026-07-20 是周一 → 上一交易日为周五 07-17
    assert engine._ca_prev_trading_day(date(2026, 7, 20)) == date(2026, 7, 17)
    # 周日 → 周五
    assert engine._ca_prev_trading_day(date(2026, 7, 19)) == date(2026, 7, 17)


def test_timeline_digest_stable_and_sensitive():
    def _tl(ratio):
        return CorporateActionTimeline([
            CorporateActionEvent(code="600000.SH", event_type="dividend",
                                 ex_date=date(2026, 7, 20),
                                 cash_per_share=ratio),
            CorporateActionEvent(code="600000.SH", event_type="split",
                                 ex_date=date(2026, 7, 21),
                                 share_ratio=ratio),
        ])

    a, b = _tl("0.35"), _tl("0.35")
    assert a.digest() == b.digest()
    assert a.digest() != _tl("0.36").digest()
    assert len(a) == 2
    assert CorporateActionTimeline([]).is_empty()


def test_events_up_to_handles_postpone_and_window():
    events = [CorporateActionEvent(code="600000.SH", event_type="dividend",
                                   ex_date=date(2026, 7, 18),
                                   cash_per_share=Decimal("1"))]
    tl = CorporateActionTimeline(events)
    # 周五（事件日之前）不生效；周一（事件日之后）生效；再调用窗口为空
    assert tl.events_up_to(date(2026, 7, 17)) == []
    assert len(tl.events_up_to(date(2026, 7, 20), after=date(2026, 7, 17))) == 1
    assert tl.events_up_to(date(2026, 7, 20), after=date(2026, 7, 20)) == []
