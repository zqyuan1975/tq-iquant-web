# -*- coding: utf-8 -*-
"""公司行为回填命令行入口（服务器无 UI 时补数）。

与 ``POST /api/corporate-actions/sync`` 同一服务实现，结果字段一致：

    requested / covered / no_events / failed / added / updated / rights

只读 TQ 除权表、只写 ``corporate_actions`` 与覆盖清单；不触发下单、不改持仓。
默认只补「未回填」代码（幂等）；``--include-covered`` 才按范围全量刷新。
被 TQ 闸（P0/交易窗口）拒绝时直接退出，不绕闸、不加锁放宽。

用法
----
    cd main
    .\\.venv\\Scripts\\python.exe scripts\\sync_corporate_actions.py --all-missing
    .\\.venv\\Scripts\\python.exe scripts\\sync_corporate_actions.py --pool-id 4
    .\\.venv\\Scripts\\python.exe scripts\\sync_corporate_actions.py --pool-id 1,3,4
    .\\.venv\\Scripts\\python.exe scripts\\sync_corporate_actions.py --codes 603187.SH,600211.SH
    .\\.venv\\Scripts\\python.exe scripts\\sync_corporate_actions.py --all-missing --start 20240101 --end 20261231

stdout 为 JSON 摘要；stderr 为一行人类可读汇总（含 failed 逐个列出）。
退出码：0 全部成功（或无需同步）；1 有单票 failed；2 参数/闸门拒绝；3 其它异常。
"""
import argparse
import json
import sys
from pathlib import Path

_MAIN = Path(__file__).resolve().parent.parent
if str(_MAIN) not in sys.path:
    sys.path.insert(0, str(_MAIN))

from core.db import SessionLocal, init_db                              # noqa: E402
from core.services.corporate_action_sync import (                     # noqa: E402
    all_pool_ids, missing_coverage_codes, record_sync_state,
    release_sync, resolve_backfill_codes, sync_corporate_actions,
    try_acquire_sync,
)


def _split_csv(raw):
    return [x.strip() for x in str(raw or "").split(",") if x.strip()]


def _parse_pool_ids(values):
    out = []
    for raw in values or []:
        for item in _split_csv(raw):
            try:
                out.append(int(item))
            except ValueError:
                raise SystemExit("--pool-id 必须是整数：%s" % item)
    return sorted(set(out))


def _scope_view(scope, missing):
    return {
        "pool_ids": scope["pool_ids"],
        "pool_counts": {str(k): len(v) for k, v in scope["by_pool"].items()},
        "holdings": scope["holdings"],
        "missing_filtered": missing,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="公司行为回填（TQ 除权数据）")
    parser.add_argument("--pool-id", action="append", default=None,
                        help="股票池 id，可重复或用逗号分隔（如 1,3,4）")
    parser.add_argument("--codes", default="",
                        help="显式股票代码，逗号分隔")
    parser.add_argument("--all-missing", action="store_true",
                        help="全部股票池中未回填的代码")
    parser.add_argument("--include-covered", action="store_true",
                        help="不做缺失过滤（按范围全量刷新，默认只补缺失）")
    parser.add_argument("--start", default="", help="起始日期（默认全历史）")
    parser.add_argument("--end", default="", help="结束日期（默认全历史）")
    args = parser.parse_args(argv)

    codes = _split_csv(args.codes)
    pool_ids = _parse_pool_ids(args.pool_id)
    if not codes and not pool_ids and not args.all_missing:
        parser.error("请指定 --codes、--pool-id 或 --all-missing 之一")

    init_db()
    from core.tq import gate as tq_gate
    from core.tq.gate import P2, TdxTaskBlocked, tdx_priority

    db = SessionLocal()
    try:
        scope = None
        if not codes:
            scope = resolve_backfill_codes(
                db, pool_ids=pool_ids or all_pool_ids(db))
            codes = scope["codes"]
        if not codes:
            print("回填范围为空：请检查股票池是否已导入", file=sys.stderr)
            return 2

        missing = len(missing_coverage_codes(db, codes))
        if not args.include_covered:
            codes = missing_coverage_codes(db, codes)
        if not codes:
            record_sync_state(
                "cli", "skipped",
                scope=_scope_view(scope, missing) if scope else None,
                message="无缺失代码")
            print(json.dumps({
                "requested": 0, "covered": [], "no_events": [], "failed": [],
                "added": 0, "updated": 0, "rights": 0,
                "missing_filtered": 0, "skipped": True,
            }, ensure_ascii=False, indent=2, default=str))
            print("[OK] 当前无缺失代码，无需同步", file=sys.stderr)
            return 0

        policy = tq_gate.evaluate_policy()
        if not policy.get("p2_allowed"):
            nxt = policy.get("next_p2_open_at")
            when = nxt.strftime("%m-%d %H:%M") if hasattr(nxt, "strftime") else "收盘后"
            print("公司行为回填在交易时段暂停（实盘优先），%s 后可用" % when,
                  file=sys.stderr)
            return 2

        if not try_acquire_sync():
            print("公司行为同步正在进行中，请稍后重试", file=sys.stderr)
            return 2

        try:
            try:
                with tdx_priority(P2):
                    result = sync_corporate_actions(
                        db, codes, start=args.start, end=args.end)
            except TdxTaskBlocked:
                print("公司行为回填被 TQ 闸拒绝（交易时段），未做任何取数",
                      file=sys.stderr)
                return 2
        finally:
            release_sync()

        scope_view = (_scope_view(scope, missing) if scope is not None else None)
        record_sync_state(
            "cli", "failed" if result["failed"] else "success",
            result=result, scope=scope_view)
        if scope_view is not None:
            result["scope"] = scope_view
        elif not args.include_covered:
            result["missing_filtered"] = missing

        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        print("[%s] requested=%d covered=%d no_events=%d failed=%d "
              "added=%d updated=%d rights=%d"
              % ("FAIL" if result["failed"] else "OK",
                 result["requested"], len(result["covered"]),
                 len(result["no_events"]), len(result["failed"]),
                 result["added"], result["updated"], result["rights"]),
              file=sys.stderr)
        for code in result["failed"]:
            print("  failed: %s" % code, file=sys.stderr)
        return 1 if result["failed"] else 0
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001 — 脚本层兜底，保留可读原因
        print("公司行为同步失败：%s: %s" % (type(exc).__name__, exc),
              file=sys.stderr)
        return 3
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
