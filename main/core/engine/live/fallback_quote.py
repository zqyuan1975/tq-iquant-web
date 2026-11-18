"""J5：行情取数降级链（默认关闭，显式开启才生效）。

背景：BarPoller 的 1m 行情只绑一个 dispatcher（tdx/ths 快照合成、iquant 桥、
掘金桥），单一源异常/返回空时整轮行情直接缺失，盘中策略与风控静默停摆。
本包装按配置的优先级链依次尝试，前一个源抛异常或返回空才切下一个；成功返回
的每根 bar 带 ``source=<provider>`` 标记，便于事后区分行情来源。

边界（不改变既有实盘语义）：
- 默认关闭（``live.quote_fallback.enabled=false``）：LiveEngine/BarPoller 完全
  不感知本模块，行为与接入前逐字一致；
- 只包行情读方法（``query_quote`` / ``query_quotes_batch``），委托、持仓、
  撤单等交易方法不经此包装——绝不因行情降级改变下单通道；
- 全部源失败时，优先透传 ``BridgeUnavailableError``（BarPoller 据此置离线，
  保持原有断线语义）；非桥异常则抛最后一个（不吞异常）；
- ``bars_explicitly_completed`` 取所有源的 AND（保守）：任一源不能保证
  「返回即闭合」时，BarPoller 仍按「最新一根视为进行中」的相对规则判定。
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..http_bridge_dispatcher import BridgeUnavailableError

logger = logging.getLogger("core.engine.live.fallback_quote")

Provider = Tuple[str, Any]


class FallbackQuoteDispatcher:
    """按优先级链尝试行情源；成功结果附加 ``source`` 标记。"""

    def __init__(self, providers: Iterable[Provider]) -> None:
        self._providers: List[Provider] = [
            (str(name), disp) for name, disp in providers if disp is not None
        ]
        if not self._providers:
            raise ValueError("FallbackQuoteDispatcher 至少需要一个行情源")
        # 最近一次成功取数的源名（观测用；不参与任何交易判定）
        self.last_source: Optional[str] = None
        self._warned: set = set()

    @property
    def sources(self) -> List[str]:
        return [name for name, _ in self._providers]

    # ---------------- BarPoller/MarketDataService 读取的能力属性 ----------------
    @property
    def supports_batch_quote(self) -> bool:
        return any(bool(getattr(d, "supports_batch_quote", False))
                   for _, d in self._providers)

    @property
    def supports_historical_quote(self) -> bool:
        """以主源为准：断线补派语义不能因备用源变化而改变。"""
        return bool(getattr(self._providers[0][1], "supports_historical_quote", False))

    @property
    def bars_explicitly_completed(self) -> bool:
        return all(bool(getattr(d, "bars_explicitly_completed", False))
                   for _, d in self._providers)

    @property
    def synthesizer(self):
        """主源的快照合成器（MarketDataService 自动发现用）。"""
        return getattr(self._providers[0][1], "synthesizer", None)

    def __getattr__(self, name: str):
        """未显式定义的只读属性透传主源（如 last_error），避免包装改变观测语义。"""
        primary = self.__dict__.get("_providers", [])
        if not primary:
            raise AttributeError(name)
        return getattr(primary[0][1], name)

    # ---------------- 行情读 ----------------
    def query_quote(self, code: str, period: str = "1m", count: int = 10) -> list:
        errors: List[Exception] = []
        for name, disp in self._providers:
            try:
                bars = disp.query_quote(code, period=period, count=count)
            except BridgeUnavailableError as e:
                errors.append(e)
                continue
            except Exception as e:  # noqa: BLE001 — 单源故障切下一个
                errors.append(e)
                logger.warning("quote fallback: %s 取数失败 %s", name, e)
                continue
            if bars:
                self.last_source = name
                if name != self._providers[0][0]:
                    self._note_fallback(name)
                return [_tag(b, name) for b in bars]
        if errors:
            raise _pick_error(errors)
        return []

    def query_quotes_batch(
        self, codes: List[str], period: str, count: int,
    ) -> Dict[str, list]:
        """批量取数：按链依次补齐缺失代码；支持批量时一次取，否则逐只。"""
        remaining = list(codes)
        out: Dict[str, list] = {}
        errors: List[Exception] = []
        for name, disp in self._providers:
            if not remaining:
                break
            try:
                got = self._fetch_provider(disp, remaining, period, count)
            except BridgeUnavailableError as e:
                errors.append(e)
                continue
            except Exception as e:  # noqa: BLE001
                errors.append(e)
                logger.warning("quote fallback: %s 批量取数失败 %s", name, e)
                continue
            filled = [c for c in remaining if got.get(c)]
            if not filled:
                continue
            self.last_source = name
            if name != self._providers[0][0]:
                self._note_fallback(name)
            for code in filled:
                out[code] = [_tag(b, name) for b in got[code]]
            remaining = [c for c in remaining if c not in out]
        if out:
            return out
        if errors:
            raise _pick_error(errors)
        return {}

    @staticmethod
    def _fetch_provider(
        disp: Any, codes: List[str], period: str, count: int,
    ) -> Dict[str, list]:
        if bool(getattr(disp, "supports_batch_quote", False)):
            batch = disp.query_quotes_batch(codes, period, count)
            if batch:
                return batch
        return {
            code: disp.query_quote(code, period=period, count=count)
            for code in codes
        }

    def _note_fallback(self, name: str) -> None:
        """主源失败切换备用源：每个源只记一次 WARNING，避免 5s 轮询刷屏。"""
        if name in self._warned:
            return
        self._warned.add(name)
        logger.warning(
            "quote fallback: 主源不可用/返回空，已切换到 %s（后续相同切换不重复提示）",
            name,
        )


def _tag(bar: Any, source: str) -> Any:
    """bar dict 附加来源标记；非 dict 原样返回（不改造数据源对象）。"""
    if not isinstance(bar, dict):
        return bar
    tagged = dict(bar)
    tagged.setdefault("source", source)
    return tagged


def _pick_error(errors: List[Exception]) -> Exception:
    """失败出口：桥不可用优先透传（保持 BarPoller 置离线语义），否则抛最后一个。"""
    for err in errors:
        if isinstance(err, BridgeUnavailableError):
            return err
    if errors:
        return errors[-1]
    return BridgeUnavailableError("所有行情源均返回空")
