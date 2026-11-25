"""股票池 Service 层（P1 #9 续块）。

承接 core.api.stock_pools 的业务逻辑：本地池序列化、通达信板块合并查询、
成分股实时查询、按 code upsert + 全量替换成分股、删除（含 RESTRICT 引用拦截）。

异常约定（路由层翻译为 HTTP 状态码）：
- TDXConnectionError：通达信不可达 → 路由 err(500)
- LookupError：板块在通达信不存在 → 路由 err(404)
- IntegrityError：池被组合策略引用（ondelete=RESTRICT）→ 路由 err(409)

路由层仅剩 HTTP 入口 + 资源校验(404) + 异常翻译 + ok/err 包装。
"""
from datetime import date

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from core.models import StockPool, StockPoolMembership, StockPoolStock
from core.tq.data import TQData
from core.tq.utils import TDXConnectionError


def _today() -> date:
    """同步日/默认生效日（单测可 monkeypatch 本模块的 ``_today``）。"""
    return date.today()


def _norm_day(value):
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        raise ValueError("日期格式应为 YYYY-MM-DD")


def serialize_pool(db: Session, p: StockPool) -> dict:
    """本地 StockPool → dict，含 stock_count（显式二次查询，模型无 relationship）。"""
    count = db.query(StockPoolStock).filter(StockPoolStock.pool_id == p.id).count()
    return {
        "id": p.id,
        "code": p.code,
        "name": p.name,
        "synced_at": p.synced_at,
        "stock_count": count,
    }


def list_local_pools(db: Session) -> list[dict]:
    pools = db.query(StockPool).order_by(StockPool.id).all()
    return [serialize_pool(db, p) for p in pools]


def list_tdx_pools(db: Session) -> list[dict]:
    """直读通达信用户板块（get_user_sector），合并本地残留（通达信已删但本地还在的）。

    返回 [{code, name, synced, exists_in_tdx, stock_count, origin}]：
    - synced: 本地是否已同步
    - exists_in_tdx: 通达信是否还有此板块（False=本地残留）
    - stock_count: 本地成分股数（未同步为 0）
    - origin: tdx=通达信板块/残留；internal=平台内部池（SEL_ 前缀，选股发布生成，
      从不回写通达信，不能按「通达信已删除」展示）

    抛 TDXConnectionError：通达信不可达。
    """
    tdx_pools = TQData().get_stock_pools()  # 抛 TDXConnectionError

    local_pools = db.query(StockPool).all()
    local_by_code = {p.code: p for p in local_pools}
    tdx_codes = {t["code"] for t in tdx_pools}
    internal_prefix = "SEL_"

    result = []
    # 通达信板块
    for t in tdx_pools:
        p = local_by_code.get(t["code"])
        count = (
            db.query(StockPoolStock).filter(StockPoolStock.pool_id == p.id).count()
            if p else 0
        )
        result.append({
            "code": t["code"],
            "name": t["name"],
            "synced": p is not None,
            "exists_in_tdx": True,
            "stock_count": count,
            "origin": "tdx",
        })
    # 本地残留（通达信侧已不存在）
    for p in local_pools:
        if p.code not in tdx_codes:
            count = db.query(StockPoolStock).filter(StockPoolStock.pool_id == p.id).count()
            internal = p.code.startswith(internal_prefix)
            result.append({
                "code": p.code,
                "name": p.name,
                "synced": True,
                "exists_in_tdx": False,
                "stock_count": count,
                "origin": "internal" if internal else "tdx",
            })
    return result


def list_tdx_stocks(code: str) -> list[dict]:
    """实时成分股。

    抛 LookupError：板块在通达信不存在。
    抛 TDXConnectionError：通达信不可达。
    """
    tdx = TQData()
    sectors = tdx.get_stock_pools()
    if not any(s["code"] == code for s in sectors):
        raise LookupError("板块不存在")
    return tdx.get_pool_stocks(code)


def sync_pool(db: Session, code: str) -> dict:
    """按 code upsert：查通达信拿 name + 成分股，本地有则更新无则新建，全量替换成分股。

    抛 LookupError：板块在通达信不存在。
    抛 TDXConnectionError：通达信不可达。
    """
    tdx = TQData()
    sectors = tdx.get_stock_pools()
    sector = next((s for s in sectors if s["code"] == code), None)
    if sector is None:
        raise LookupError("板块不存在")
    tdx_stocks = tdx.get_pool_stocks(code)

    p = db.query(StockPool).filter(StockPool.code == code).first()
    if p is None:
        p = StockPool(code=code, name=sector["name"])
        db.add(p)
        db.flush()
    else:
        p.name = sector["name"]

    # 全量替换成分股
    db.query(StockPoolStock).filter(StockPoolStock.pool_id == p.id).delete()
    codes = []
    for s in tdx_stocks:
        stock_code = s.get("stock_code")
        if stock_code:
            codes.append(stock_code)
            db.add(StockPoolStock(
                pool_id=p.id,
                stock_code=stock_code,
                stock_name=s.get("stock_name"),
            ))
    # 时点成分（J7）：同步只做「新增→开区间、消失→关区间」，历史已关闭记录不重开。
    # 组合未开 pool_pit_enabled 时本表不参与选股，写它只为以后开启时点池时有据可查。
    sync_memberships(db, p.id, codes, source="sync", day=_today())
    p.synced_at = func.now()
    db.commit()
    db.refresh(p)
    return serialize_pool(db, p)


# ---------------------------------------------------------------------------
# 时点股票池成员（J7，2026-11-18）
# ---------------------------------------------------------------------------
def serialize_membership(m: StockPoolMembership) -> dict:
    return {
        "id": m.id,
        "pool_id": m.pool_id,
        "stock_code": m.stock_code,
        "effective_from": m.effective_from,
        "effective_to": m.effective_to,
        "source": m.source,
        "note": m.note,
        "is_current": m.effective_to is None,
    }


def list_memberships(db: Session, pool_id: int, as_of: date = None) -> list[dict]:
    """池成员区间列表。``as_of`` 非空时只返回该日有效的成员。"""
    if as_of is not None:
        day = _norm_day(as_of)
        rows = (
            db.query(StockPoolMembership)
            .filter(
                StockPoolMembership.pool_id == pool_id,
                StockPoolMembership.effective_from <= day,
                (StockPoolMembership.effective_to.is_(None))
                | (StockPoolMembership.effective_to >= day),
            )
            .order_by(StockPoolMembership.stock_code,
                      StockPoolMembership.effective_from)
            .all()
        )
        return [serialize_membership(m) for m in rows]
    rows = (
        db.query(StockPoolMembership)
        .filter(StockPoolMembership.pool_id == pool_id)
        .order_by(StockPoolMembership.stock_code,
                  StockPoolMembership.effective_from)
        .all()
    )
    return [serialize_membership(m) for m in rows]


def _overlap_query(db: Session, pool_id: int, code: str, start: date,
                   end, exclude_id: int = None):
    """既有区间 [ef, et] 与新区间 [start, end] 是否重叠（et=None 视为开区间）。"""
    q = db.query(StockPoolMembership).filter(
        StockPoolMembership.pool_id == pool_id,
        StockPoolMembership.stock_code == code,
    )
    if end is not None:
        q = q.filter(StockPoolMembership.effective_from <= end)
    q = q.filter(
        (StockPoolMembership.effective_to.is_(None))
        | (StockPoolMembership.effective_to >= start)
    )
    if exclude_id is not None:
        q = q.filter(StockPoolMembership.id != exclude_id)
    return q.first()


def sync_memberships(db: Session, pool_id: int, codes, source: str = "sync",
                     day: date = None) -> dict:
    """按成分 diff 写生效区间（不 commit，由调用方提交）。

    新增 → ``effective_from=day`` / ``effective_to=NULL``；消失 → 给现役记录填
    ``effective_to=day``（含当日，fail-safe：同步日不因口径变化误拦）。
    已在池内且现役的记录保持原 ``effective_from``，不重开、不改写。
    """
    day = _norm_day(day) or _today()
    wanted = {str(c).strip() for c in (codes or []) if str(c or "").strip()}
    open_rows = (
        db.query(StockPoolMembership)
        .filter(
            StockPoolMembership.pool_id == pool_id,
            StockPoolMembership.effective_to.is_(None),
        )
        .all()
    )
    open_by_code = {m.stock_code: m for m in open_rows}
    added = closed = 0
    for code in sorted(wanted - set(open_by_code)):
        db.add(StockPoolMembership(
            pool_id=pool_id, stock_code=code, effective_from=day,
            effective_to=None, source=source))
        added += 1
    for code, row in open_by_code.items():
        if code not in wanted:
            row.effective_to = day
            closed += 1
    db.flush()
    return {"added": added, "closed": closed, "current": len(wanted)}


def create_membership(db: Session, pool_id: int, payload: dict) -> dict:
    """手工新增区间。校验：code 非空、from 必填、to>=from、同代码区间不重叠。"""
    pool = db.query(StockPool).filter(StockPool.id == pool_id).first()
    if pool is None:
        raise LookupError("股票池不存在")
    code = str(payload.get("stock_code") or "").strip()
    if not code:
        raise ValueError("stock_code 不能为空")
    start = _norm_day(payload.get("effective_from"))
    if start is None:
        raise ValueError("effective_from 不能为空")
    end = _norm_day(payload.get("effective_to"))
    if end is not None and end < start:
        raise ValueError("effective_to 不能早于 effective_from")
    if _overlap_query(db, pool_id, code, start, end) is not None:
        raise ValueError("该股票在此区间与既有生效记录重叠")
    row = StockPoolMembership(
        pool_id=pool_id, stock_code=code, effective_from=start,
        effective_to=end, source=str(payload.get("source") or "manual"),
        note=payload.get("note"))
    db.add(row)
    db.commit()
    db.refresh(row)
    return serialize_membership(row)


def update_membership(db: Session, membership_id: int, payload: dict):
    """改区间（None 不动）。重叠校验排除自身；不存在返回 None。"""
    row = (
        db.query(StockPoolMembership)
        .filter(StockPoolMembership.id == membership_id)
        .first()
    )
    if row is None:
        return None
    code = str(payload.get("stock_code") or row.stock_code).strip()
    start = (_norm_day(payload["effective_from"])
             if "effective_from" in payload else row.effective_from)
    end = (_norm_day(payload["effective_to"])
           if "effective_to" in payload else row.effective_to)
    if not code:
        raise ValueError("stock_code 不能为空")
    if start is None:
        raise ValueError("effective_from 不能为空")
    if end is not None and end < start:
        raise ValueError("effective_to 不能早于 effective_from")
    if _overlap_query(db, row.pool_id, code, start, end,
                      exclude_id=row.id) is not None:
        raise ValueError("该股票在此区间与既有生效记录重叠")
    row.stock_code = code
    row.effective_from = start
    row.effective_to = end
    if "note" in payload:
        row.note = payload.get("note")
    db.commit()
    db.refresh(row)
    return serialize_membership(row)


def delete_membership(db: Session, membership_id: int) -> bool:
    row = (
        db.query(StockPoolMembership)
        .filter(StockPoolMembership.id == membership_id)
        .first()
    )
    if row is None:
        return False
    db.delete(row)
    db.commit()
    return True


def delete_pool(db: Session, pool_id: int) -> bool:
    """删本地池（StockPoolStock 随 ondelete=CASCADE 删）。

    False=不存在；被组合策略引用时 ondelete=RESTRICT 抛 IntegrityError（路由 catch→409）。
    """
    p = db.query(StockPool).filter(StockPool.id == pool_id).first()
    if p is None:
        return False
    db.delete(p)
    db.commit()
    return True
