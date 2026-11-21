"""组合级扩展设置（不新增表/迁移，落 data/portfolio_settings.json）。

目前承载：
- `equity_cap_mode`（cap|distribute）
- 行业层决策开关（2026-11-22，全部默认关闭；关闭时行为与改动前逐位一致）

为什么不加列：该字段是「组合 JSON」级配置，改一次即可，加表列需迁移；文件写入
用 tmp + os.replace 原子替换，进程内加锁避免并发 PUT 写坏 JSON。文件缺失或损坏
一律按默认值处理，不影响组合读写主链路。
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Dict, Optional

from core.engine.industry_gate import INDUSTRY_MODES

# main/data/portfolio_settings.json（main/core/services → parents[2] == main）
_DEFAULT_PATH = Path(__file__).resolve().parents[2] / "data" / "portfolio_settings.json"
_LOCK = threading.Lock()
VALID_MODES = ("cap", "distribute")

#: 行业层全部开关的默认值（默认关闭 = 不改变任何既有结果）
INDUSTRY_DEFAULTS: Dict[str, object] = {
    # A 放行门控：off | relief（仅大盘空头时按行业放行）| strict（常开拦非多头）
    "industry_gate_mode": "off",
    # B 跨策略 OPEN 排序 + 同行业持仓上限
    "industry_rank_enabled": False,
    "industry_max_per_industry": 2,
    # C 行业强度作仓位权重
    "industry_weight_enabled": False,
    # D 行业超买（z>=2.5）不开新仓
    "industry_overbought_block": False,
    # E 行业转空时该行业持仓移动止损收紧
    "industry_trail_tighten_enabled": False,
    "industry_trail_tighten_ratio": 0.015,
    # F 行业空头减仓比例（0=关闭；与大盘档取 max，不叠加）
    "industry_bear_reduce_ratio": 0.0,
    # G 大盘-行业背离看板提示（仅展示，不进决策）
    "industry_divergence_hint": False,
}


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


# ---------------- 行业层决策开关（2026-11-22） ----------------

def _coerce_industry_value(key: str, value) -> object:
    """按默认值类型校验/夹取，非法值回落默认（不抛异常）。"""
    default = INDUSTRY_DEFAULTS[key]
    if key == "industry_gate_mode":
        mode = str(value or "off").strip().lower()
        return mode if mode in INDUSTRY_MODES else "off"
    if isinstance(default, bool):
        return bool(value)
    if key == "industry_max_per_industry":
        try:
            return max(0, min(100, int(value)))
        except (TypeError, ValueError):
            return default
    try:
        return min(1.0, max(0.0, float(value)))
    except (TypeError, ValueError):
        return default


def get_industry_settings(portfolio_id: int,
                          path: Optional[os.PathLike | str] = None) -> Dict[str, object]:
    """组合的行业层开关（缺失/非法一律回落默认=全部关闭）。"""
    row = _read(path).get(str(portfolio_id))
    stored = row if isinstance(row, dict) else {}
    return {
        key: _coerce_industry_value(
            key, stored[key] if key in stored else default)
        for key, default in INDUSTRY_DEFAULTS.items()
    }


def set_industry_settings(portfolio_id: int, values: Dict[str, object],
                          path: Optional[os.PathLike | str] = None) -> Dict[str, object]:
    """写入行业层开关（仅接受的键；等于默认值的键不落盘）。返回合并后的设置。"""
    with _LOCK:
        p = _path(path)
        data = _read(p)
        key = str(portfolio_id)
        row = dict(data.get(key) or {})
        for name, value in (values or {}).items():
            if name not in INDUSTRY_DEFAULTS:
                continue
            coerced = _coerce_industry_value(name, value)
            if coerced == INDUSTRY_DEFAULTS[name]:
                row.pop(name, None)
            else:
                row[name] = coerced
        if row:
            data[key] = row
        else:
            data.pop(key, None)
        if not data:
            try:
                p.unlink()
            except FileNotFoundError:
                pass
        else:
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(".json.tmp")
            tmp.write_text(
                json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8")
            os.replace(tmp, p)
        return {
            name: _coerce_industry_value(
                name, row[name] if name in row else default)
            for name, default in INDUSTRY_DEFAULTS.items()
        }
