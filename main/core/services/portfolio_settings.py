"""组合级扩展设置（不新增表/迁移，落 data/portfolio_settings.json）。

目前只承载 `equity_cap_mode`（cap|distribute）：
- cap（默认）：现有行为，equity_cap 只封顶新开仓定量；
- distribute：日终把「组合总资产 − equity_cap」作为外部流出扣出现金（仅回测生效）。

为什么不加列：该字段是「组合 JSON」级配置，改一次即可，加表列需迁移；文件写入
用 tmp + os.replace 原子替换，进程内加锁避免并发 PUT 写坏 JSON。文件缺失或损坏
一律按默认值处理（cap），不影响组合读写主链路。
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Dict, Optional

# main/data/portfolio_settings.json（main/core/services → parents[2] == main）
_DEFAULT_PATH = Path(__file__).resolve().parents[2] / "data" / "portfolio_settings.json"
_LOCK = threading.Lock()
VALID_MODES = ("cap", "distribute")


def _path(path: Optional[os.PathLike | str] = None) -> Path:
    return Path(path) if path is not None else _DEFAULT_PATH


def _read(path: Optional[os.PathLike | str] = None) -> Dict[str, dict]:
    try:
        data = json.loads(_path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def get_equity_cap_mode(portfolio_id: int, path: Optional[os.PathLike | str] = None) -> str:
    """组合的 equity_cap 处理模式；未设置/非法值一律返回 cap（旧行为）。"""
    row = _read(path).get(str(portfolio_id))
    mode = row.get("equity_cap_mode") if isinstance(row, dict) else None
    return mode if mode in VALID_MODES else "cap"


def set_equity_cap_mode(portfolio_id: int, mode: Optional[str],
                        path: Optional[os.PathLike | str] = None) -> None:
    """写入模式；mode=None/"cap" 删除键（默认值不落盘）。"""
    mode = mode if mode in VALID_MODES else None
    with _LOCK:
        p = _path(path)
        data = _read(p)
        key = str(portfolio_id)
        row = dict(data.get(key) or {})
        if mode and mode != "cap":
            row["equity_cap_mode"] = mode
        else:
            row.pop("equity_cap_mode", None)
        if row:
            data[key] = row
        else:
            data.pop(key, None)
        if not data:
            # 全部恢复默认：删文件，避免残留空 {}（也避免测试/首启创建无意义文件）
            try:
                p.unlink()
            except FileNotFoundError:
                pass
            return
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8")
        os.replace(tmp, p)


def delete_portfolio_settings(portfolio_id: int,
                              path: Optional[os.PathLike | str] = None) -> None:
    with _LOCK:
        p = _path(path)
        data = _read(p)
        if str(portfolio_id) not in data:
            return
        data.pop(str(portfolio_id), None)
        if data:
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(".json.tmp")
            tmp.write_text(
                json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8")
            os.replace(tmp, p)
        else:
            try:
                p.unlink()
            except FileNotFoundError:
                pass
