"""条件选股池反向推送到通达信自定义板块（真实写入，可回读校验）。

口径：
- 只写平台自己的池（``SEL_`` 前缀），不创建/清空/改名任何用户板块；
- 通过通达信客户端接口 create_sector / clear_sector / send_user_block 写入，
  由客户端自己落 T0002/blocknew（文件名 = 8 位板块代码），不直接改文件，
  避免客户端退出时用内存覆盖配置；
- 写入前备份 blocknew.cfg 与已有 SEL_*.blk 到 data/tdx_block_backup/<时间戳>/；
- 幂等：已有板块先 clear 再写，板块代码由池 code 稳定派生（同池永远同代码）；
- 写入后立即用 get_user_sector / get_stock_list_in_sector 回读校验，
  回读数量不一致时在结果里如实报告，不静默成功。
"""

from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from core.models.stock_pool import StockPool
from core.models.stock_pool_stock import StockPoolStock
from core.tq.utils import TDXConnectionError, _tdx_path, get_tdx_lock, get_tq

POOL_PREFIX = "SEL_"
BACKUP_ROOT = Path(__file__).resolve().parents[2] / "data" / "tdx_block_backup"
BACKUP_KEEP = 5


class TdxPushError(RuntimeError):
    """推送失败（路径缺失 / 通达信不可用 / 接口返回错误）。"""


def _block_code(pool_code: str) -> str:
    """池 code → 通达信板块代码（≤8 位，稳定可复现）。"""
    return "SEL" + hashlib.md5(pool_code.encode("utf-8")).hexdigest().upper()[:5]


def _block_name(pool_name: str) -> str:
    return ("选股·" + (pool_name or "选股池"))[:16]


def _parse(res) -> dict:
    if isinstance(res, dict):
        return res
    if isinstance(res, str) and res:
        try:
            data = json.loads(res)
            return data if isinstance(data, dict) else {}
        except ValueError:
            return {}
    return {}


def _ensure_ok(res, action: str) -> dict:
    data = _parse(res)
    error_id = data.get("ErrorId")
    if error_id not in (None, 0, "0"):
        raise TdxPushError(f"{action}失败：{data.get('Error') or error_id}")
    return data


def _backup(blocknew_dir: Path) -> str:
    """备份 blocknew.cfg 与已有 SEL_*.blk；返回备份目录（失败即中止写入）。"""
    cfg = blocknew_dir / "blocknew.cfg"
    if not cfg.exists():
        raise TdxPushError(f"未找到通达信板块配置：{cfg}（请检查 config.yaml 的 tdx_path）")
    target = BACKUP_ROOT / datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    try:
        target.mkdir(parents=True, exist_ok=True)
        shutil.copy2(cfg, target / "blocknew.cfg")
        for blk in blocknew_dir.glob("SEL*.blk"):
            shutil.copy2(blk, target / blk.name)
    except OSError as e:
        raise TdxPushError(f"备份通达信板块文件失败，已中止推送：{e}") from e
    dirs = sorted([d for d in BACKUP_ROOT.iterdir() if d.is_dir()])
    for old in dirs[:-BACKUP_KEEP]:
        shutil.rmtree(old, ignore_errors=True)
    return str(target)


def _is_series(code: str) -> bool:
    """序列池：SEL_ 开头且不以 8 位日期结尾（版本池不推送，避免板块泛滥）。"""
    if not code.startswith(POOL_PREFIX):
        return False
    tail = code.rsplit("_", 1)[-1]
    return not (len(tail) == 8 and tail.isdigit())


def push(db: Session, pool_codes: Optional[List[str]] = None,
         all_series: bool = False) -> dict:
    if all_series:
        pools = [p for p in db.query(StockPool)
                 .filter(StockPool.code.like(POOL_PREFIX + "%")).all()
                 if _is_series(p.code)]
    else:
        codes = [c for c in (pool_codes or []) if c]
        if not codes:
            raise TdxPushError("请指定要推送的池（pool_codes）或选择 all_series")
        bad = [c for c in codes if not c.startswith(POOL_PREFIX)]
        if bad:
            raise TdxPushError("只允许推送平台自己的 SEL_ 池，拒绝写入用户板块：%s"
                               % "、".join(bad))
        pools = db.query(StockPool).filter(StockPool.code.in_(codes)).all()
        missing = sorted(set(codes) - {p.code for p in pools})
        if missing:
            raise TdxPushError("本地不存在这些池：%s" % "、".join(missing))
    if not pools:
        raise TdxPushError("没有可推送的序列池")

    blocknew_dir = Path(_tdx_path()) / "T0002" / "blocknew"
    backup_dir = _backup(blocknew_dir)

    results = []
    try:
        with get_tdx_lock():
            tq = get_tq()
            existing = {s.get("Code"): s.get("Name")
                        for s in (tq.get_user_sector() or [])}
            for pool in sorted(pools, key=lambda p: p.code):
                stocks = [r.stock_code for r in db.query(StockPoolStock)
                          .filter(StockPoolStock.pool_id == pool.id)
                          .order_by(StockPoolStock.stock_code).all()]
                if not stocks:
                    results.append({"pool_code": pool.code, "block_code": _block_code(pool.code),
                                    "name": pool.name, "requested": 0, "verified": 0,
                                    "status": "skipped_empty"})
                    continue
                code, name = _block_code(pool.code), _block_name(pool.name)
                if code in existing:
                    _ensure_ok(tq.clear_sector(code), "清空板块(%s)" % code)
                    if existing.get(code) != name:  # 池改名后同步板块名
                        _ensure_ok(tq.rename_sector(code, name), "重命名板块(%s)" % code)
                else:
                    _ensure_ok(tq.create_sector(code, name), "创建板块(%s)" % code)
                _ensure_ok(tq.send_user_block(code, stocks), "写入成分(%s)" % code)
                readback = tq.get_stock_list_in_sector(code, block_type=1, list_type=1) or []
                verified = len(readback)
                results.append({
                    "pool_code": pool.code, "block_code": code, "name": name,
                    "requested": len(stocks), "verified": verified,
                    "status": "ok" if verified == len(stocks) else "partial",
                })
    except TDXConnectionError as e:
        raise TdxPushError(f"通达信不可用，未写入任何板块：{e}") from e
    except TdxPushError:
        raise
    except Exception as e:  # 接口异常：明确失败，不假装成功
        raise TdxPushError(f"推送到通达信失败：{e}") from e

    return {
        "blocks": results,
        "blocks_written": len([r for r in results if r["status"] == "ok"]),
        "blocks_partial": len([r for r in results if r["status"] == "partial"]),
        "blocks_skipped": len([r for r in results if r["status"] == "skipped_empty"]),
        "stocks_written": sum(r["verified"] for r in results),
        "target_dir": str(blocknew_dir),
        "target_files": [str(blocknew_dir / (r["block_code"] + ".blk")) for r in results
                         if r["status"] != "skipped_empty"],
        "backup_dir": backup_dir,
    }
