"""P0-2（2026-11-24）回归：非 remark 通道判废前强制复核委托表。

真实故障（2026-11-18）：301600 卖出 220 股，180s 内没认领到 order_ref 即被判
rejected，事后无成交回报。TDX 的 remark 恒空，旧逻辑判废前既不重拉委托表、也不做
legacy 模糊匹配，且把「查询失败」与「确实不存在」同等对待；判 rejected 会释放
同股同向在途门 → 旧单仍在柜台时后续风控卖出会再报一张（双份成交，真钱风险）。

覆盖：
- 委托表命中（legacy 代码+方向+数量+时间窗）→ 认领 order_ref，不判废；
- 委托表查询抛 BridgeUnavailableError → 保持 pending（查不到 ≠ 不存在）；
- 查询成功且确无候选 → rejected，error_message 写明复核依据与时间；
- 仅 fabricated 候选 → 不绑定，仍 rejected（防冒领手工/遗留单）。
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.http_bridge_dispatcher import BridgeUnavailableError
from core.engine.live.order_machine import OrderStateMachine, _UNTRACEABLE_PENDING_MARKER
from core.models import Base, LiveOrder


def _db_factory():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


class _TdxLikeDispatcher:
    """模拟通达信：supports_remark=False，query_orders 无 remark/cl_ord_id。"""

    supports_remark = False
    supports_cancel = True

    def __init__(self, rows=None, exc=None):
        self._rows = rows if rows is not None else []
        self._exc = exc
        self.queries = 0

    def query_orders(self, order_id=None):
        self.queries += 1
        if self._exc is not None:
            raise self._exc
        return list(self._rows)

    def query_deals(self, order_id=None):
        return []


def _machine(disp):
    ctx = SimpleNamespace(session_id=1, dispatcher=disp)
    return OrderStateMachine(ctx, lambda *a, **k: None)


def _row(db, *, status="submitted", age_seconds=200, code="301600.SZ",
         side="sell", qty=220):
    created = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
        seconds=age_seconds)
    row = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1,
        stock_code=code, trade_type=side, order_type="limit",
        price=Decimal("8.10"), quantity=qty, filled_quantity=0,
        status=status, bridge_order_id="oid_tdx_301600",
        created_at=created,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _candidate(row, *, ref="A9001", fabricated=False, volume=None,
               direction=49):
    # 柜台插入时间用上海本地时间（UTC+8）表达 same instant，落在时间窗内。
    local = row.created_at + timedelta(hours=8)
    return {
        "source": "BRIDGE",
        "remark": "",
        "order_ref": ref,
        "instrument": "301600",
        "exchange": "SZ",
        "direction": direction,
        "volume": volume if volume is not None else int(row.quantity),
        "insert_date": local.strftime("%Y%m%d"),
        "insert_time": local.strftime("%H%M%S"),
        "insert_fabricated": fabricated,
        "status": 50,
    }


def test_legacy_trace_claims_ref_instead_of_reject():
    factory = _db_factory()
    db = factory()
    row = _row(db)
    disp = _TdxLikeDispatcher(rows=[_candidate(row)])
    machine = _machine(disp)

    machine.expire_stale_orders(db, [row])
    db.commit()

    assert row.status == "submitted", "委托表命中的单不得判废"
    assert row.order_ref == "A9001"
    assert disp.queries >= 1
    db.close()


def test_query_failure_keeps_pending():
    factory = _db_factory()
    db = factory()
    row = _row(db)
    disp = _TdxLikeDispatcher(exc=BridgeUnavailableError("read timeout"))
    machine = _machine(disp)

    machine.expire_stale_orders(db, [row])
    db.commit()

    assert row.status == "submitted", "查不到 ≠ 不存在：查询失败必须保持 pending"
    assert str(row.error_message or "").startswith(_UNTRACEABLE_PENDING_MARKER)
    assert "查询失败" in (row.error_message or "")
    db.close()


def test_confirmed_absent_rejects_with_evidence():
    factory = _db_factory()
    db = factory()
    row = _row(db)
    disp = _TdxLikeDispatcher(rows=[])
    machine = _machine(disp)

    machine.expire_stale_orders(db, [row])
    db.commit()

    assert row.status == "rejected"
    assert "order match timeout" in (row.error_message or "")
    assert "已复核委托表" in (row.error_message or "")
    db.close()


def test_fabricated_candidate_not_bound():
    """陌生 ref 的补记时间不可信，不得冒领（防绑到旧单/手工单）。"""
    factory = _db_factory()
    db = factory()
    row = _row(db)
    disp = _TdxLikeDispatcher(
        rows=[_candidate(row, ref="FAB001", fabricated=True)])
    machine = _machine(disp)

    machine.expire_stale_orders(db, [row])
    db.commit()

    assert row.status == "rejected"
    assert row.order_ref is None
    db.close()


def test_other_side_candidate_not_bound():
    factory = _db_factory()
    db = factory()
    row = _row(db)  # sell → direction 49
    disp = _TdxLikeDispatcher(
        rows=[_candidate(row, direction=48, ref="BUY001")])
    machine = _machine(disp)

    machine.expire_stale_orders(db, [row])
    db.commit()

    assert row.status == "rejected"
    assert row.order_ref is None
    db.close()
