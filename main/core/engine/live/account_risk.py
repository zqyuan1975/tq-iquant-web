"""账户（会话）级风控聚合（D1/D2，2026-11-18）。

与组合级 ``PortfolioRiskManager`` **独立**：组合级看单组合净值，本服务看
会话内所有组合的风险口径净值之和（现金 + 持仓市值 − 累计外部净流入）。

默认关闭（``live_sessions.account_risk_enabled=0``）：``on_bar`` 第一行返回，
不读取组合、不写状态、不发事件，引擎行为与接线前逐字一致。

触发语义：
- 账户最大回撤 / 日亏达到阈值 → ``halted=True``，引擎对本组合本批 BUY 剥单
  （SELL/风控卖出不受影响）；触发日次一交易日自动解除。
- 累计触发 3 次转 ``manual_recovery``，需人工恢复。
- 60%/80% 为分级预警（每交易日每级一次），100% 为触发。
"""
from __future__ import annotations

import json
import logging
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Callable, Dict, Optional

from sqlalchemy.orm import Session

from ...models import LiveDecisionEvent
from ...services.notify_service import notify
from ..format import fmt_money, fmt_pct
from .context import EngineContext

logger = logging.getLogger("core.engine.live.account_risk")

DEFAULT_MAX_DRAWDOWN = Decimal("0.15")
DEFAULT_DAILY_LOSS_LIMIT = Decimal("0.05")
# 分级预警：相对熔断阈值的比例（60% / 80% 预警，100% 触发）
WARN_RATIOS = (Decimal("0.60"), Decimal("0.80"))


def _dec(value) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


class AccountRiskService:
    """会话级聚合风控：推进账户净值序列、分级预警、熔断/日亏停开仓。"""

    def __init__(
        self,
        ctx: EngineContext,
        emit: Callable[[str, dict], None],
        config: Optional[dict] = None,
    ) -> None:
        cfg = config or {}
        self._ctx = ctx
        self._emit = emit
        self.enabled = bool(cfg.get("enabled"))
        self.max_drawdown = _dec(cfg.get("max_drawdown")) or DEFAULT_MAX_DRAWDOWN
        self.daily_loss_limit = (
            _dec(cfg.get("daily_loss_limit")) or DEFAULT_DAILY_LOSS_LIMIT)
        # 内存态（跨日/重启语义对齐组合级 max_drawdown / daily_loss）
        self.peak_value = Decimal("0")
        self.prev_close_value: Optional[Decimal] = None
        self.last_date: Optional[date] = None
        self.breaker_active = False
        self.breaker_trigger_date: Optional[date] = None
        self.daily_pause_active = False
        self.daily_loss_trigger_date: Optional[date] = None
        self.triggers = 0
        self.manual_recovery = False
        self._last_total: Optional[Decimal] = None
        self._last_bar_time = None
        self._dirty = False
        # 已预警级别：{"YYYY-MM-DD:kind:level": True}，跨日自动清理
        self._fired: Dict[str, bool] = {}
        raw_state = cfg.get("state")
        if raw_state:
            self.restore(raw_state)

    # ---------------- 判定 ----------------
    @property
    def halted(self) -> bool:
        """是否停新开仓（账户熔断或日亏暂停任一生效）。"""
        if not self.enabled:
            return False
        return self.breaker_active or self.daily_pause_active

    def _account_risk_value(self) -> Decimal:
        """Σ 各组合风险口径净值（无 bar 时以最近已知价估值）。"""
        total = Decimal("0")
        for port in (self._ctx.portfolios or []):
            try:
                total += port.risk_equity()
            except Exception:  # noqa: BLE001 — 单组合估值异常不拖垮账户聚合
                logger.warning(
                    "account risk: portfolio %s 估值失败，按 0 计入",
                    getattr(port, "portfolio_id", None), exc_info=True)
        return total

    def on_bar(self, bar_time, is_baseline: bool = False) -> None:
        """每根 bar 调用（同一 bar_time 只算一次）。"""
        if not self.enabled or is_baseline or bar_time is None:
            return
        if self._last_bar_time == bar_time:
            return
        self._last_bar_time = bar_time
        day = bar_time.date() if hasattr(bar_time, "date") else bar_time
        total = self._account_risk_value()
        if total <= 0:
            return
        # 1. 跨日：昨收刷新、日亏暂停解除、熔断次日恢复、清理过期预警
        if self.last_date is not None and self.last_date != day:
            self.prev_close_value = self._last_total
            self.daily_pause_active = False
            self.daily_loss_trigger_date = None
            if (self.breaker_active and not self.manual_recovery
                    and self.breaker_trigger_date is not None
                    and day > self.breaker_trigger_date):
                self.breaker_active = False
                self._dirty = True
                self._record_decision(
                    "max_drawdown", "recover", day, None, None,
                    "账户级 max_drawdown 熔断次日自动恢复（触发日 %s）"
                    % self.breaker_trigger_date,
                )
                notify(
                    "warn", "账户熔断自动恢复",
                    "会话 %s 账户级 max_drawdown 熔断已次日自动恢复（解除停新开仓）"
                    % (self._ctx.session_id,),
                    gate="account_breaker_recovered",
                    code=str(self._ctx.session_id), trade_date=day,
                    fields={"会话": self._ctx.session_id},
                )
            self._fired = {
                k: v for k, v in self._fired.items()
                if k.startswith(str(day))
            }
        self.last_date = day
        self._last_total = total
        # 2. 更新峰值（只增不减，与组合级一致）
        if total > self.peak_value:
            self.peak_value = total
            self._dirty = True
        # 3. 最大回撤：分级预警 + 触发
        if self.peak_value > 0 and self.max_drawdown > 0:
            drawdown = (self.peak_value - total) / self.peak_value
            self._maybe_warn("max_drawdown", drawdown, self.max_drawdown, day,
                             total)
            if (not self.breaker_active and not self.manual_recovery
                    and drawdown >= self.max_drawdown):
                self.breaker_active = True
                self.breaker_trigger_date = day
                self.triggers += 1
                if self.triggers >= 3:
                    self.manual_recovery = True
                self._dirty = True
                logger.warning(
                    "account risk: session %s max_drawdown 触发 peak=%s "
                    "total=%s drawdown=%s threshold=%s (第 %d 次%s)",
                    self._ctx.session_id, fmt_money(self.peak_value),
                    fmt_money(total), fmt_pct(drawdown),
                    fmt_pct(self.max_drawdown), self.triggers,
                    "，转手动恢复" if self.manual_recovery else "",
                )
        # 4. 账户日亏：分级预警 + 暂停（分母 = 昨收净值）
        if (self.prev_close_value is not None and self.prev_close_value > 0
                and self.daily_loss_limit > 0):
            pnl = total - self.prev_close_value
            if pnl < 0:
                loss = abs(pnl) / self.prev_close_value
                self._maybe_warn("daily_loss", loss, self.daily_loss_limit,
                                 day, total)
                if (not self.daily_pause_active
                        and loss >= self.daily_loss_limit):
                    self.daily_pause_active = True
                    self.daily_loss_trigger_date = day
                    self._dirty = True
                    logger.warning(
                        "account risk: session %s daily_loss 触发 total=%s "
                        "prev_close=%s pnl=%s loss=%s threshold=%s → 当日暂停新开仓",
                        self._ctx.session_id, fmt_money(total),
                        fmt_money(self.prev_close_value), fmt_money(pnl),
                        fmt_pct(loss), fmt_pct(self.daily_loss_limit),
                    )
        if self._dirty:
            self.persist_state()

    def _maybe_warn(self, kind: str, actual: Decimal, threshold: Decimal,
                    day: date, total: Decimal) -> None:
        """按 60/80/100% 分级告警（每交易日每级一次）。"""
        if threshold <= 0:
            return
        ratio = actual / threshold
        level = None
        for warn_ratio in WARN_RATIOS:
            if ratio >= warn_ratio:
                level = str(int(warn_ratio * 100))
        if ratio >= 1:
            level = "100"
        if level is None:
            return
        key = "%s:%s:%s" % (day, kind, level)
        if key in self._fired:
            return
        self._fired[key] = True
        self._dirty = True
        name = "最大回撤" if kind == "max_drawdown" else "日内亏损"
        severity = "critical" if level == "100" else "warn"
        message = "账户级%s%s：当前 %s，阈值 %s（触发 %s%% 分档）" % (
            name, "触发" if level == "100" else "预警",
            fmt_pct(actual), fmt_pct(threshold), level)
        self._emit("risk", {
            "session_id": self._ctx.session_id,
            "rule": "account_%s" % kind,
            "level": level,
            "triggered": level == "100",
            "total_value": str(total),
            "actual_pct": float(actual),
            "threshold_pct": float(threshold),
            "message": message,
        })
        notify(
            severity, "账户级%s%s" % (name, "触发" if level == "100" else "预警"),
            message,
            gate="account_%s" % kind,
            code=str(self._ctx.session_id), trade_date=day,
            fields={"会话": self._ctx.session_id,
                    "账户净值": fmt_money(total),
                    "当前": fmt_pct(actual),
                    "阈值": fmt_pct(threshold)},
        )
        self._record_decision(
            kind, "halt" if level == "100" else "warn", day, actual,
            threshold, message,
        )

    # ---------------- 人工恢复 / 配置 ----------------
    def recover(self, reason: Optional[str] = None) -> None:
        """人工恢复账户级熔断：清零计数、解除手动恢复与当日暂停。"""
        self.breaker_active = False
        self.manual_recovery = False
        self.breaker_trigger_date = None
        self.triggers = 0
        self.daily_pause_active = False
        self.daily_loss_trigger_date = None
        self._dirty = True
        day = self.last_date
        message = "账户级熔断已人工恢复（计数清零）%s" % (
            "：%s" % reason if reason else "")
        self._record_decision("max_drawdown", "recover", day, None, None,
                              message)
        notify(
            "warn", "账户熔断已手动恢复", message,
            gate="account_breaker_manual_recovered",
            code=str(self._ctx.session_id), trade_date=day,
            fields={"会话": self._ctx.session_id},
        )
        self.persist_state()

    def apply_config(self, enabled: Optional[bool] = None,
                     max_drawdown=None, daily_loss_limit=None) -> None:
        """运行中即时更新账户级开关/阈值（仅影响后续 bar 判定）。"""
        if enabled is not None:
            self.enabled = bool(enabled)
        if max_drawdown is not None:
            self.max_drawdown = _dec(max_drawdown) or DEFAULT_MAX_DRAWDOWN
        if daily_loss_limit is not None:
            self.daily_loss_limit = (
                _dec(daily_loss_limit) or DEFAULT_DAILY_LOSS_LIMIT)
        self._dirty = True

    # ---------------- 状态持久化 ----------------
    def state_payload(self) -> str:
        def _num(v):
            return None if v is None else str(v)

        def _day(v):
            return None if v is None else v.isoformat()

        return json.dumps({
            "schema": 1,
            "peak_value": _num(self.peak_value),
            "prev_close_value": _num(self.prev_close_value),
            "last_date": _day(self.last_date),
            "last_total": _num(self._last_total),
            "breaker_active": bool(self.breaker_active),
            "breaker_trigger_date": _day(self.breaker_trigger_date),
            "daily_pause_active": bool(self.daily_pause_active),
            "daily_loss_trigger_date": _day(self.daily_loss_trigger_date),
            "triggers": int(self.triggers),
            "manual_recovery": bool(self.manual_recovery),
            "fired": dict(self._fired),
        }, ensure_ascii=False)

    def restore(self, raw: str) -> bool:
        """读回状态；坏数据静默忽略（不阻断启动）。"""
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            return False
        if not isinstance(data, dict):
            return False
        if int(data.get("schema") or 0) < 1:
            return False
        try:
            peak = _dec(data.get("peak_value"))
            prev = _dec(data.get("prev_close_value"))
            last_total = _dec(data.get("last_total"))
            last_date = data.get("last_date")
            breaker_day = data.get("breaker_trigger_date")
            daily_day = data.get("daily_loss_trigger_date")
            fired = data.get("fired") or {}
            triggers = int(data.get("triggers") or 0)
        except (TypeError, ValueError):
            return False
        self.peak_value = peak if peak is not None else Decimal("0")
        self.prev_close_value = prev
        self._last_total = last_total
        self.last_date = date.fromisoformat(last_date) if last_date else None
        self.breaker_trigger_date = (
            date.fromisoformat(breaker_day) if breaker_day else None)
        self.daily_loss_trigger_date = (
            date.fromisoformat(daily_day) if daily_day else None)
        self.breaker_active = bool(data.get("breaker_active"))
        self.daily_pause_active = bool(data.get("daily_pause_active"))
        self.triggers = triggers
        self.manual_recovery = bool(data.get("manual_recovery"))
        if isinstance(fired, dict):
            self._fired = {str(k): True for k in fired}
        return True

    def persist_state(self) -> None:
        """把内存态写回 live_sessions.account_risk_state（失败只告警）。"""
        self._dirty = False
        db: Optional[Session] = None
        try:
            from core.models import LiveSession
            db = self._ctx.db_session_factory()
            row = db.query(LiveSession).filter(
                LiveSession.id == self._ctx.session_id).first()
            if row is None:
                return
            row.account_risk_state = self.state_payload()
            db.commit()
        except Exception:  # noqa: BLE001 — 观测状态写失败不阻断交易
            if db is not None:
                db.rollback()
            logger.warning(
                "account risk persist state failed (session %s)",
                self._ctx.session_id, exc_info=True)
        finally:
            if db is not None:
                db.close()

    def _record_decision(self, kind: str, action: str, day: Optional[date],
                         actual: Optional[Decimal],
                         threshold: Optional[Decimal], message: str) -> None:
        """落 live_decision_events（gate=account_risk），供 M4 后续 ack 展示。"""
        db: Optional[Session] = None
        try:
            db = self._ctx.db_session_factory()
            db.add(LiveDecisionEvent(
                live_session_id=self._ctx.session_id,
                gate="account_risk", layer="account_risk", action=action,
                bar_time=None, param_name=kind,
                param_value=float(threshold) if threshold is not None else None,
                actual_value=float(actual) if actual is not None else None,
                message=message[:200],
            ))
            db.commit()
        except Exception:  # noqa: BLE001
            if db is not None:
                db.rollback()
            logger.warning(
                "account risk decision event failed (session %s)",
                self._ctx.session_id, exc_info=True)
        finally:
            if db is not None:
                db.close()
