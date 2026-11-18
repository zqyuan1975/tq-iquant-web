"""告警外发通道（飞书自定义机器人 Webhook）。

设计边界（重要）
----------------
- **默认关闭**：``config.yaml`` 无 ``notify`` 段或 ``notify.enabled=false`` 时，
  ``notify()`` 立即返回 False——不建线程、不发网络请求，行为与接入前逐字一致。
- **绝不阻塞/不影响交易主链路**：``notify()`` 只做「级别过滤 + 静默期去重 + 入队」，
  真正的 HTTP 发送在独立 daemon 线程里做（超时 3~5 秒）。任何异常都在内部吞掉
  并只写日志——所有调用点都在既有事件已经发射/落库之后。
- **不改既有语义**：本模块不改任何落库、不改闸门、不改订单状态机；调用点只允许
  「在既有事件产生的同一处顺带发一条」。

分级：``info < warn < error < critical``，低于 ``min_level`` 的事件直接丢弃。

去重：同一 ``(gate, code, trade_date)`` 在 ``silence_seconds`` 静默期内只发一条
（默认 300 秒），避免轮询类异常刷屏；``code``/``trade_date`` 缺省时退化为
``(gate, '', '')``，即同闸门同静默期只发一条。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import httpx

from ..config import load_config

logger = logging.getLogger(__name__)

#: 级别 → 权重（比较大小用）
LEVELS: Dict[str, int] = {
    "info": 10,
    "warn": 20,
    "error": 30,
    "critical": 40,
}
DEFAULT_MIN_LEVEL = "warn"
DEFAULT_SILENCE_SECONDS = 300.0
DEFAULT_TIMEOUT_SECONDS = 5.0
#: 发送队列上限：满则丢弃新事件（交易主链路绝不因告警堆积而阻塞/等待）
QUEUE_MAX = 200
#: 发送失败告警节流（秒）：失败只写日志，不刷屏
FAIL_LOG_INTERVAL_SECONDS = 600.0
#: 静默期表上限（超出清空，防长期运行内存增长）
_DEDUPE_MAX = 2000


def level_value(level: str) -> int:
    """级别字符串 → 权重；未知级别按 ``warn`` 处理（宁可发出也不静默丢弃）。"""
    return LEVELS.get(str(level or "").strip().lower(), LEVELS["warn"])


@dataclass
class NotifyConfig:
    """``notify`` 配置段（全部有安全默认值，缺键/非法值都回退）。"""

    enabled: bool = False
    channel: str = "feishu"
    webhook_url: str = ""
    secret: str = ""
    min_level: str = DEFAULT_MIN_LEVEL
    silence_seconds: float = DEFAULT_SILENCE_SECONDS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS

    @property
    def usable(self) -> bool:
        """启用且配置了 webhook 地址才算可用（缺地址按关闭处理）。"""
        return bool(self.enabled and self.webhook_url)


def _as_bool(raw: Any, default: bool = False) -> bool:
    if raw is None:
        return default
    if isinstance(raw, str):
        return raw.strip().lower() in ("1", "true", "yes", "on")
    return bool(raw)


def _as_float(raw: Any, default: float) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return float(default)
    return value if value > 0 else float(default)


def from_config(cfg: Optional[dict] = None) -> NotifyConfig:
    """从整份 config 解析 ``notify`` 段；任何异常都回退默认（关闭）。

    ``TQ_NOTIFY_DISABLED=1``（测试/调试）：强制关闭，绝不向真实 Webhook 发消息。
    """
    try:
        if str(os.environ.get("TQ_NOTIFY_DISABLED") or "").strip().lower() in (
                "1", "true", "yes", "on"):
            return NotifyConfig()
        cfg = load_config() if cfg is None else cfg
        section = (cfg or {}).get("notify") or {}
        if not isinstance(section, dict):
            return NotifyConfig()
        timeout = _as_float(
            section.get("timeout_seconds"), DEFAULT_TIMEOUT_SECONDS)
        # 超时必须短：告警链路永远不许拖住调用方（秒级上限 3~5s）
        timeout = min(max(timeout, 1.0), 5.0)
        return NotifyConfig(
            enabled=_as_bool(section.get("enabled"), False),
            channel=str(section.get("channel") or "feishu").strip().lower(),
            webhook_url=str(section.get("webhook_url") or "").strip(),
            secret=str(section.get("secret") or "").strip(),
            min_level=str(section.get("min_level") or DEFAULT_MIN_LEVEL).strip().lower(),
            silence_seconds=_as_float(
                section.get("silence_seconds"), DEFAULT_SILENCE_SECONDS),
            timeout_seconds=timeout,
        )
    except Exception:  # noqa: BLE001 — 配置读坏不能影响调用方
        logger.warning("notify: 解析 notify 配置失败，按关闭处理", exc_info=True)
        return NotifyConfig()


class NotifyError(RuntimeError):
    """发送失败（HTTP 错误 / 飞书返回非 0 code）。"""


def mask_sensitive(value: str, keep: int = 4) -> str:
    """敏感串脱敏：只留尾部 ``keep`` 位（日志/异常里永远不出现完整值）。"""
    text = str(value or "")
    if not text:
        return "—"
    return "***%s" % text[-keep:] if len(text) > keep else "***"


def _sanitize(text: str, cfg: NotifyConfig) -> str:
    """把文本中可能带出的 webhook 地址与 secret 替换为脱敏形式。"""
    out = str(text or "")
    for raw, masked in ((cfg.webhook_url, mask_sensitive(cfg.webhook_url, 6)),
                        (cfg.secret, mask_sensitive(cfg.secret))):
        if raw and raw in out:
            out = out.replace(raw, masked)
    return out


class FeishuSender:
    """飞书自定义机器人 Webhook 发送器（文本 / 富文本 post）。

    - 无 ``secret`` → 直接 POST（无签名机器人）
    - 有 ``secret`` → 按飞书规范加 ``timestamp`` + ``sign``
    - ``timeout`` 强制 ≤5 秒，超时/网络错误一律抛 ``NotifyError``，由调用方吞掉
    """

    def __init__(self, webhook_url: str, secret: str = "",
                 timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
                 client_factory=None):
        self.webhook_url = webhook_url
        self.secret = secret
        self.timeout_seconds = min(max(float(timeout_seconds), 1.0), 5.0)
        # 测试注入点：返回带 post() 方法的对象（默认 httpx.Client）
        self._client_factory = client_factory or (
            lambda: httpx.Client(timeout=self.timeout_seconds))

    def _signed_payload(self, payload: dict) -> dict:
        if not self.secret:
            return payload
        timestamp = str(int(time.time()))
        key = ("%s\n%s" % (timestamp, self.secret)).encode("utf-8")
        digest = hmac.new(key, b"", digestmod=hashlib.sha256).digest()
        body = dict(payload)
        body["timestamp"] = timestamp
        body["sign"] = base64.b64encode(digest).decode("utf-8")
        return body

    def _post(self, payload: dict) -> dict:
        body = self._signed_payload(payload)
        try:
            with self._client_factory() as client:
                resp = client.post(self.webhook_url, json=body)
            if getattr(resp, "status_code", 200) >= 400:
                raise NotifyError("HTTP %s" % resp.status_code)
            data = resp.json()
        except NotifyError:
            raise
        except Exception as exc:  # noqa: BLE001 — 统一收敛为 NotifyError
            raise NotifyError("%s: %s" % (type(exc).__name__, exc))
        code = (data or {}).get("code")
        if code not in (0, None):
            raise NotifyError("飞书返回 code=%s msg=%s" % (code, data.get("msg")))
        return data

    def send_text(self, text: str) -> dict:
        return self._post({
            "msg_type": "text",
            "content": {"text": text},
        })

    def send_rich(self, title: str, lines: List[str]) -> dict:
        content = [[{"tag": "text", "text": line}] for line in lines if line]
        return self._post({
            "msg_type": "post",
            "content": {"post": {"zh_cn": {"title": title, "content": content}}},
        })


def _today_text(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value or "")


@dataclass
class NotifyEvent:
    """入队的一条告警（发送线程按此组装消息）。"""

    level: str
    title: str
    message: str
    fields: Dict[str, str] = field(default_factory=dict)


class Notifier:
    """级别过滤 + 静默期去重 + 后台发送（daemon 线程，惰性启动）。"""

    def __init__(self, cfg: Optional[NotifyConfig] = None,
                 sender_factory=None, mono_fn=time.monotonic):
        self._cfg = cfg
        self._sender_factory = sender_factory or (
            lambda c: FeishuSender(c.webhook_url, c.secret, c.timeout_seconds))
        self._mono = mono_fn
        self._queue: "queue.Queue[NotifyEvent]" = queue.Queue(maxsize=QUEUE_MAX)
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        # (gate, code, trade_date) -> 上次发出时刻（单调时钟）
        self._last_sent: Dict[tuple, float] = {}
        self._last_fail_log = 0.0
        self._dropped = 0

    # ------------------------------------------------------------ 配置
    def config(self) -> NotifyConfig:
        """每次取最新配置（改 config.yaml 无需重启；非法值回退关闭）。"""
        if self._cfg is not None:
            return self._cfg
        return from_config()

    # ------------------------------------------------------------ 对外入口
    def notify(self, level: str, title: str, message: str, *,
               gate: Optional[str] = None, code: Optional[str] = None,
               trade_date: Any = None,
               fields: Optional[Dict[str, Any]] = None) -> bool:
        """过滤 → 去重 → 入队。返回是否真的入队。**绝不抛异常。**"""
        try:
            cfg = self.config()
            if not cfg.usable:
                return False
            if level_value(level) < level_value(cfg.min_level):
                return False
            key = (str(gate or title or "")[:60], str(code or "")[:20],
                   _today_text(trade_date))
            now = self._mono()
            with self._lock:
                last = self._last_sent.get(key)
                if last is not None and (now - last) < cfg.silence_seconds:
                    return False
                self._last_sent[key] = now
                if len(self._last_sent) > _DEDUPE_MAX:
                    self._last_sent.clear()
                    self._last_sent[key] = now
            text_fields = {str(k): str(v) for k, v in (fields or {}).items()
                           if v is not None and str(v) != ""}
            event = NotifyEvent(
                level=str(level or "warn").lower(),
                title=str(title or "实盘告警"),
                message=str(message or ""),
                fields=text_fields,
            )
            try:
                self._queue.put_nowait(event)
            except queue.Full:
                self._dropped += 1
                logger.warning(
                    "notify: 发送队列已满，丢弃告警 gate=%s（累计丢弃 %d）",
                    key[0], self._dropped)
                return False
            self._ensure_thread()
            return True
        except Exception:  # noqa: BLE001 — 告警链路绝不影响调用方
            logger.warning("notify: 入队失败 level=%s title=%s",
                           level, title, exc_info=True)
            return False

    def close(self, timeout: float = 1.0) -> None:
        """停止发送线程（正常不调用；进程退出随 daemon 线程消失）。"""
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
        self._thread = None

    # ------------------------------------------------------------ 后台发送
    def _ensure_thread(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run, name="notify-sender", daemon=True)
            self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                event = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            self._send_one(event)

    def _send_one(self, event: NotifyEvent) -> None:
        """发送一条；失败只写日志（按 600s 节流），绝不抛出。"""
        cfg = self.config()
        if not cfg.usable:
            return
        try:
            sender = self._sender_factory(cfg)
            if event.fields:
                lines = [event.message] if event.message else []
                lines.extend("%s：%s" % (k, v) for k, v in event.fields.items())
                sender.send_rich(
                    "[%s] %s" % (event.level.upper(), event.title), lines)
            else:
                text = event.message or event.title
                sender.send_text("[%s] %s" % (event.level.upper(), text))
        except Exception as exc:  # noqa: BLE001 — 发送失败不影响主链路
            now = self._mono()
            if now - self._last_fail_log >= FAIL_LOG_INTERVAL_SECONDS:
                self._last_fail_log = now
                logger.warning(
                    "notify: 告警外发失败（%s）；已节流日志 %d 秒，交易主链路不受影响",
                    _sanitize(str(exc), cfg), int(FAIL_LOG_INTERVAL_SECONDS))


#: 进程内单例（惰性创建；配置每次重新读，便于改 config.yaml 即时生效）
_NOTIFIER: Optional[Notifier] = None
_NOTIFIER_LOCK = threading.Lock()


def get_notifier() -> Notifier:
    global _NOTIFIER
    with _NOTIFIER_LOCK:
        if _NOTIFIER is None:
            _NOTIFIER = Notifier()
        return _NOTIFIER


def reset_notifier(notifier: Optional[Notifier] = None) -> None:
    """替换/清理进程内单例（测试注入用）。"""
    global _NOTIFIER
    with _NOTIFIER_LOCK:
        _NOTIFIER = notifier


def notify(level: str, title: str, message: str, *,
           gate: Optional[str] = None, code: Optional[str] = None,
           trade_date: Any = None, fields: Optional[Dict[str, Any]] = None) -> bool:
    """模块级便捷入口：**任何情况下都不抛**（内部已全部吞异常）。"""
    try:
        return get_notifier().notify(
            level, title, message, gate=gate, code=code,
            trade_date=trade_date, fields=fields)
    except Exception:  # noqa: BLE001 — 单例构造失败也不得影响调用方
        return False
