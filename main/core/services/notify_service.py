"""告警外发通道（飞书 / 企微群机器人 / SMTP 邮件 / 本地提示音）。

设计边界（重要）
----------------
- **默认关闭**：``config.yaml`` 无 ``notify`` 段或 ``notify.enabled=false`` 时，
  ``notify()`` 立即返回 False——不建线程、不发网络请求，行为与接入前逐字一致。
- **多渠道可选**：飞书为既有渠道（``channel/webhook_url/secret`` 语义不变）；
  企微 / 邮件 / 提示音为可选段，缺省全部为空或关闭，不配置时行为与接入前一致。
  一条告警同时发往所有已配置渠道，任一成功即视为外发成功（单渠道故障不影响其它）。
- **绝不阻塞/不影响交易主链路**：``notify()`` 只做「级别过滤 + 静默期去重 + 入队」，
  真正的发送在独立 daemon 线程里做（HTTP 超时 3~5 秒）。任何异常都在内部吞掉
  并只写日志——所有调用点都在既有事件已经发射/落库之后。
- **不改既有语义**：本模块不改任何落库、不改闸门、不改订单状态机；调用点只允许
  「在既有事件产生的同一处顺带发一条」。
- **限频不丢**：飞书返回 ``code=11232 frequency limited`` 时不再只写日志丢弃，
  改为落本地待发队列（``runtime.data_dir()/notify_pending.jsonl``，原子替换）并按
  指数退避重试；关键事件（卖出/风控/熔断）按去重键合并、降频重投；进程重启后
  由 ``Notifier.start()`` 恢复队列继续投递。其它失败仍按原口径只节流写日志。

分级：``info < warn < error < critical``，低于 ``min_level`` 的事件直接丢弃。

去重：同一 ``(gate, code, trade_date)`` 在 ``silence_seconds`` 静默期内只发一条
（默认 300 秒），避免轮询类异常刷屏；``code``/``trade_date`` 缺省时退化为
``(gate, '', '')``，即同闸门同静默期只发一条。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import queue
import smtplib
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import httpx

from ..config import load_config
from ..runtime import data_dir

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

# ---- 飞书限频（code=11232 frequency limited）落盘重试 ----
#: 飞书自定义机器人限频错误码：命中后不丢，转本地待发队列按退避重试
RATE_LIMIT_CODE = "11232"
#: 退避基数/上限（秒）：30 → 60 → 120 → 240 → 300
RETRY_BASE_SECONDS = 30.0
RETRY_MAX_SECONDS = 300.0
#: 关键事件（卖出/风控/熔断）限频后的合并重投间隔（秒）：降频，不丢弃
RETRY_KEY_SECONDS = 300.0
#: 待发队列上限（超出丢最旧的非关键项并留日志）
PENDING_MAX = 500
#: 关键事件标记：级别=critical，或 gate/title 命中下列词（中英）
_KEY_EVENT_MARKERS = (
    "卖出", "风控", "熔断", "止损", "止盈", "子单丢失",
    "sell", "risk", "circuit", "breaker",
)
#: 待发队列默认落盘位置（相对 runtime.data_dir()，可用 TQ_NOTIFY_PENDING_DISABLED=1 关闭）
PENDING_FILE_REL = "notify_pending.jsonl"


def level_value(level: str) -> int:
    """级别字符串 → 权重；未知级别按 ``warn`` 处理（宁可发出也不静默丢弃）。"""
    return LEVELS.get(str(level or "").strip().lower(), LEVELS["warn"])


def is_rate_limited(exc: BaseException) -> bool:
    """异常是否为飞书限频（code=11232 frequency limited）。"""
    text = str(exc or "").lower()
    return RATE_LIMIT_CODE in text or "frequency limited" in text


def is_key_event(level: str, gate: str = "", title: str = "") -> bool:
    """是否关键事件（卖出/风控/熔断）：限频时合并降频重投，绝不丢弃。

    判据：级别 ≥ critical，或 gate/title 命中 :data:`_KEY_EVENT_MARKERS`
    （现有调用点里熔断/止盈止损 gate 与标题都带这些词）。
    """
    if level_value(level) >= LEVELS["critical"]:
        return True
    text = ("%s %s" % (gate or "", title or "")).lower()
    return any(mark in text for mark in _KEY_EVENT_MARKERS)


def _pending_file_disabled() -> bool:
    return str(os.environ.get("TQ_NOTIFY_PENDING_DISABLED") or "").strip().lower() in (
        "1", "true", "yes", "on")


def default_pending_path() -> Optional[Path]:
    """待发队列默认落盘路径（runtime.data_dir()/notify_pending.jsonl）；禁用/异常→None。

    与体检游标、台账同口径：源码模式落 ``main/data``，打包/TQ_APP_ROOT 模式落
    ``app_root()/data``，不再按仓库根 ``data/`` 各自推算。
    """
    if _pending_file_disabled():
        return None
    try:
        return data_dir() / PENDING_FILE_REL
    except Exception:  # noqa: BLE001 — 路径不可得不落盘（内存重试仍然生效）
        return None


@dataclass
class NotifyConfig:
    """``notify`` 配置段（全部有安全默认值，缺键/非法值都回退）。

    ``channel`` / ``webhook_url`` / ``secret`` 保持飞书旧语义不变；
    企微 / 邮件 / 本地提示音为批次 21（L1）新增的可选渠道，缺省全部为空或关闭，
    未配置时 ``usable`` 与旧实现逐字一致（仅飞书 webhook 可用才发）。
    """

    enabled: bool = False
    channel: str = "feishu"
    webhook_url: str = ""
    secret: str = ""
    min_level: str = DEFAULT_MIN_LEVEL
    silence_seconds: float = DEFAULT_SILENCE_SECONDS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    # ---- 企微群机器人 ----
    wecom_webhook_url: str = ""
    # ---- 邮件（SMTP） ----
    email_host: str = ""
    email_port: int = 465
    email_user: str = ""
    email_password: str = ""
    email_from: str = ""
    email_to: Tuple[str, ...] = ()
    email_use_ssl: bool = True
    # ---- 本地提示音 ----
    sound_enabled: bool = False
    sound_path: str = ""

    @property
    def feishu_usable(self) -> bool:
        return bool(self.enabled and self.webhook_url)

    @property
    def wecom_usable(self) -> bool:
        return bool(self.enabled and self.wecom_webhook_url)

    @property
    def email_usable(self) -> bool:
        return bool(self.enabled and self.email_host and self.email_to)

    @property
    def sound_usable(self) -> bool:
        return bool(self.enabled and self.sound_enabled)

    @property
    def usable(self) -> bool:
        """启用且至少一个渠道可用才算可用（缺全部渠道按关闭处理）。"""
        return bool(self.feishu_usable or self.wecom_usable
                    or self.email_usable or self.sound_usable)


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


def _as_port(raw: Any, default: int = 465) -> int:
    try:
        port = int(raw)
    except (TypeError, ValueError):
        return default
    return port if 0 < port < 65536 else default


def _as_list(raw: Any) -> Tuple[str, ...]:
    """收件人：支持 list/tuple 或逗号/分号分隔字符串；空项丢弃。"""
    if raw is None:
        return ()
    items = raw if isinstance(raw, (list, tuple)) else str(raw).replace(";", ",").split(",")
    return tuple(str(x).strip() for x in items if str(x).strip())


def _sub(raw: Any) -> dict:
    return raw if isinstance(raw, dict) else {}


def from_config(cfg: Optional[dict] = None) -> NotifyConfig:
    """从整份 config 解析 ``notify`` 段；任何异常都回退默认（关闭）。

    ``TQ_NOTIFY_DISABLED=1``（测试/调试）：强制关闭，绝不向真实 Webhook 发消息。
    可选渠道（企微/邮件/提示音）缺省全部为空或关闭；未配置时解析结果与旧实现一致。
    邮件密码可用环境变量 ``TQ_NOTIFY_EMAIL_PASSWORD`` 覆盖（避免明文入库/入 git）。
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
        wecom = _sub(section.get("wecom"))
        email = _sub(section.get("email"))
        sound = _sub(section.get("sound"))
        email_password = (os.environ.get("TQ_NOTIFY_EMAIL_PASSWORD")
                          or email.get("password")
                          or section.get("email_password") or "")
        return NotifyConfig(
            enabled=_as_bool(section.get("enabled"), False),
            channel=str(section.get("channel") or "feishu").strip().lower(),
            webhook_url=str(section.get("webhook_url") or "").strip(),
            secret=str(section.get("secret") or "").strip(),
            min_level=str(section.get("min_level") or DEFAULT_MIN_LEVEL).strip().lower(),
            silence_seconds=_as_float(
                section.get("silence_seconds"), DEFAULT_SILENCE_SECONDS),
            timeout_seconds=timeout,
            wecom_webhook_url=str(
                wecom.get("webhook_url")
                or section.get("wecom_webhook_url") or "").strip(),
            email_host=str(
                email.get("smtp_host") or email.get("host")
                or section.get("email_host") or "").strip(),
            email_port=_as_port(email.get("smtp_port") or email.get("port")),
            email_user=str(
                email.get("username") or email.get("user") or "").strip(),
            email_password=str(email_password).strip(),
            email_from=str(
                email.get("from") or email.get("from_addr") or "").strip(),
            email_to=_as_list(email.get("to")),
            email_use_ssl=_as_bool(email.get("use_ssl"), True),
            sound_enabled=_as_bool(sound.get("enabled"), False),
            sound_path=str(sound.get("path") or "").strip(),
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
    """把文本中可能带出的 webhook 地址、secret 与邮件密码替换为脱敏形式。"""
    out = str(text or "")
    for raw, masked in ((cfg.webhook_url, mask_sensitive(cfg.webhook_url, 6)),
                        (cfg.wecom_webhook_url,
                         mask_sensitive(cfg.wecom_webhook_url, 6)),
                        (cfg.secret, mask_sensitive(cfg.secret)),
                        (cfg.email_password, mask_sensitive(cfg.email_password))):
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


class WecomSender:
    """企微群机器人 Webhook 发送器（text / markdown）。

    企微成功返回 ``{"errcode": 0, "errmsg": "ok"}``；任何异常统一收敛为
    ``NotifyError``，由 ``Notifier`` 吞掉——与企业微信的可用性无关。
    """

    def __init__(self, webhook_url: str,
                 timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
                 client_factory=None):
        self.webhook_url = webhook_url
        self.timeout_seconds = min(max(float(timeout_seconds), 1.0), 5.0)
        self._client_factory = client_factory or (
            lambda: httpx.Client(timeout=self.timeout_seconds))

    def _post(self, payload: dict) -> dict:
        try:
            with self._client_factory() as client:
                resp = client.post(self.webhook_url, json=payload)
            if getattr(resp, "status_code", 200) >= 400:
                raise NotifyError("HTTP %s" % resp.status_code)
            data = resp.json()
        except NotifyError:
            raise
        except Exception as exc:  # noqa: BLE001 — 统一收敛为 NotifyError
            raise NotifyError("%s: %s" % (type(exc).__name__, exc))
        errcode = (data or {}).get("errcode")
        if errcode not in (0, None):
            raise NotifyError("企微返回 errcode=%s errmsg=%s"
                              % (errcode, (data or {}).get("errmsg")))
        return data

    def send_text(self, text: str) -> dict:
        return self._post({"msgtype": "text", "text": {"content": text}})

    def send_rich(self, title: str, lines: List[str]) -> dict:
        body = ["**%s**" % title] + ["> %s" % line for line in lines if line]
        return self._post({"msgtype": "markdown",
                           "markdown": {"content": "\n".join(body)}})


class EmailSender:
    """SMTP 邮件发送器（465 默认 SSL；use_ssl=false 走 STARTTLS）。

    连接/登录/发信超时均 ≤ ``timeout_seconds``，失败抛 ``NotifyError``。
    ``smtp_factory(host, port, timeout)`` 为测试注入点。
    """

    def __init__(self, host: str, port: int, user: str = "", password: str = "",
                 to: Sequence[str] = (), sender: str = "",
                 use_ssl: bool = True,
                 timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
                 smtp_factory=None):
        self.host = host
        self.port = int(port)
        self.user = user or ""
        self.password = password or ""
        self.to = tuple(x for x in to if x)
        self.sender = sender or self.user or "tq-iquant"
        self.use_ssl = bool(use_ssl)
        self.timeout_seconds = min(max(float(timeout_seconds), 1.0), 15.0)
        self._smtp_factory = smtp_factory or self._default_smtp

    def _default_smtp(self, host, port, timeout):
        if self.use_ssl:
            return smtplib.SMTP_SSL(host, port, timeout=timeout)
        return smtplib.SMTP(host, port, timeout=timeout)

    def _send(self, subject: str, body: str) -> None:
        msg = EmailMessage()
        msg["Subject"] = str(subject or "实盘告警")[:120]
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.to)
        msg.set_content(str(body or ""))
        try:
            client = self._smtp_factory(
                self.host, self.port, self.timeout_seconds)
            try:
                if not self.use_ssl:
                    client.starttls()
                if self.user:
                    client.login(self.user, self.password)
                client.send_message(msg)
            finally:
                try:
                    client.quit()
                except Exception:  # noqa: BLE001 — quit 失败不影响已发送结果
                    pass
        except NotifyError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise NotifyError("邮件发送失败 %s: %s" % (type(exc).__name__, exc))

    def send_text(self, text: str) -> None:
        first = (str(text or "").splitlines() or ["实盘告警"])[0]
        self._send(first, text)

    def send_rich(self, title: str, lines: List[str]) -> None:
        self._send(title, "\n".join(line for line in lines if line))


class SoundSender:
    """本地提示音（Windows winsound；其它平台抛 NotifyError 由上层节流记日志）。

    ``path`` 为空 → 系统提示音；提供 wav 路径 → 异步播放该文件。
    ``player`` 为测试注入点（接收 path，path 为空表示系统音）。
    """

    def __init__(self, path: str = "", player=None):
        self.path = str(path or "")
        self._player = player or self._default_play

    def _default_play(self, path: str) -> None:
        try:
            import winsound  # 仅 Windows 提供
        except Exception as exc:  # noqa: BLE001
            raise NotifyError("当前平台不支持本地提示音：%s" % exc)
        if path and Path(path).exists():
            winsound.PlaySound(str(path),
                               winsound.SND_FILENAME | winsound.SND_ASYNC)
        else:
            winsound.MessageBeep(winsound.MB_ICONHAND)

    def _play(self) -> None:
        self._player(self.path)

    def send_text(self, text: str) -> None:
        self._play()

    def send_rich(self, title: str, lines: List[str]) -> None:
        self._play()


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
    gate: str = ""
    code: str = ""
    trade_date: str = ""


class Notifier:
    """级别过滤 + 静默期去重 + 后台发送（daemon 线程，惰性启动）。

    ``sender_factory`` 仅构造飞书发送器（保持旧签名/旧测试注入点不变）；
    企微 / 邮件 / 提示音各有独立注入点，缺省用真实实现。
    一条告警按配置同时发往所有可用渠道，任一成功即视为外发成功。
    """

    def __init__(self, cfg: Optional[NotifyConfig] = None,
                 sender_factory=None, mono_fn=time.monotonic,
                 wecom_factory=None, email_factory=None, sound_factory=None,
                 pending_path=None):
        self._cfg = cfg
        self._sender_factory = sender_factory or (
            lambda c: FeishuSender(c.webhook_url, c.secret, c.timeout_seconds))
        self._wecom_factory = wecom_factory or (
            lambda c: WecomSender(c.wecom_webhook_url, c.timeout_seconds))
        self._email_factory = email_factory or (
            lambda c: EmailSender(
                c.email_host, c.email_port, c.email_user, c.email_password,
                c.email_to, c.email_from, c.email_use_ssl, c.timeout_seconds))
        self._sound_factory = sound_factory or (
            lambda c: SoundSender(c.sound_path))
        self._mono = mono_fn
        self._queue: "queue.Queue[NotifyEvent]" = queue.Queue(maxsize=QUEUE_MAX)
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        # (gate, code, trade_date) -> 上次发出时刻（单调时钟）
        self._last_sent: Dict[tuple, float] = {}
        self._last_fail_log = 0.0
        self._dropped = 0
        # 飞书限频待发队列（内存 + 落盘，进程重启后继续投递）：
        #   记录 = {level,title,message,fields,gate,code,trade_date,count,attempts,
        #           created_at,_key,_next_at}；下划线键不落盘。
        self._pending_path = Path(pending_path) if pending_path else None
        self._pending: List[dict] = []
        self._pending_loaded = False
        self._pending_lock = threading.Lock()

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
                gate=str(gate or ""),
                code=str(code or ""),
                trade_date=_today_text(trade_date),
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

    def start(self) -> None:
        """启动发送线程并恢复待发队列（进程重启后继续投递限频遗留告警）。

        配置未启用/无可用渠道时零副作用（不建线程、不读写待发文件）。
        """
        try:
            if not self.config().usable:
                return
            self._ensure_thread()
        except Exception:  # noqa: BLE001 — 启动失败不得影响调用方
            logger.warning("notify: 启动发送线程失败（忽略）", exc_info=True)

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
        # 进程重启后先把上次限频遗留的待发告警捞回来（文件不存在/已禁用时为空）
        self._load_pending()
        while not self._stop.is_set():
            try:
                try:
                    event = self._queue.get(timeout=0.5)
                except queue.Empty:
                    event = None
                if event is not None:
                    self._send_one(event)
                self._retry_due_pending()
            except Exception:  # noqa: BLE001 — 发送线程绝不因单条异常退出
                logger.warning("notify: 发送线程本轮异常（继续运行）", exc_info=True)

    def _build_senders(self, cfg: NotifyConfig) -> List[Any]:
        """按配置返回本次要投递的渠道（顺序固定：飞书 → 企微 → 邮件 → 提示音）。"""
        senders: List[Any] = []
        if cfg.feishu_usable:
            senders.append(self._sender_factory(cfg))
        if cfg.wecom_usable:
            senders.append(self._wecom_factory(cfg))
        if cfg.email_usable:
            senders.append(self._email_factory(cfg))
        if cfg.sound_usable:
            senders.append(self._sound_factory(cfg))
        return senders

    def _deliver(self, cfg: NotifyConfig, event: NotifyEvent):
        """把一条告警投递到所有可用渠道；返回 ``(成功渠道数, 异常列表)``。"""
        if not cfg.usable:
            return 0, []
        try:
            senders = self._build_senders(cfg)
        except Exception as exc:  # noqa: BLE001 — 构造失败按全失败处理
            return 0, [exc]
        if not senders:
            return 0, []
        title = "[%s] %s" % (event.level.upper(), event.title)
        errors: List[BaseException] = []
        delivered = 0
        for sender in senders:
            try:
                if event.fields:
                    lines = [event.message] if event.message else []
                    lines.extend("%s：%s" % (k, v) for k, v in event.fields.items())
                    sender.send_rich(title, lines)
                else:
                    text = event.message or event.title
                    sender.send_text("[%s] %s" % (event.level.upper(), text))
                delivered += 1
            except Exception as exc:  # noqa: BLE001 — 单渠道失败不影响其它渠道
                errors.append(exc)
        return delivered, errors

    def _send_one(self, event: NotifyEvent) -> None:
        """一条告警发往所有可用渠道；绝不抛出。

        - 任一渠道成功即视为外发成功（单渠道故障不影响其它渠道送达）；
        - **全部失败且为飞书限频（11232）→ 转本地待发队列**，按退避重试，
          关键事件合并降频，进程重启后继续投递；
        - 其它全部失败 → 保持旧行为：只按 600s 节流写日志。
        """
        cfg = self.config()
        if not cfg.usable:
            return
        delivered, errors = self._deliver(cfg, event)
        if delivered or not errors:
            return
        if is_rate_limited(errors[0]):
            self._park_pending(event)
            return
        self._log_fail(cfg, errors[0])

    # ------------------------------------------------------ 限频待发队列
    def _pending_file(self) -> Optional[Path]:
        if self._pending_path is not None:
            return self._pending_path
        return default_pending_path()

    @staticmethod
    def _event_to_record(event: NotifyEvent, count: int = 1,
                         attempts: int = 0) -> dict:
        return {
            "level": event.level,
            "title": event.title,
            "message": event.message,
            "fields": dict(event.fields),
            "gate": event.gate,
            "code": event.code,
            "trade_date": event.trade_date,
            "count": int(count),
            "attempts": int(attempts),
            "created_at": datetime.now().timestamp(),
            "_key": (event.gate[:60], event.code[:20], event.trade_date),
            "_next_at": 0.0,
        }

    @staticmethod
    def _record_to_event(rec: dict) -> NotifyEvent:
        raw_fields = rec.get("fields")
        fields = {str(k): str(v) for k, v in raw_fields.items()} \
            if isinstance(raw_fields, dict) else {}
        return NotifyEvent(
            level=str(rec.get("level") or "warn"),
            title=str(rec.get("title") or "实盘告警"),
            message=str(rec.get("message") or ""),
            fields=fields,
            gate=str(rec.get("gate") or ""),
            code=str(rec.get("code") or ""),
            trade_date=str(rec.get("trade_date") or ""),
        )

    def _backoff_seconds(self, attempts: int) -> float:
        return min(RETRY_MAX_SECONDS, RETRY_BASE_SECONDS * (2 ** max(0, attempts)))

    def _park_pending(self, event: NotifyEvent) -> None:
        """限频告警转入待发队列（落盘 + 退避）；关键事件按 key 合并降频。"""
        now = self._mono()
        key = (event.gate[:60], event.code[:20], event.trade_date)
        key_event = is_key_event(event.level, event.gate, event.title)
        with self._pending_lock:
            existing = next(
                (r for r in self._pending if r.get("_key") == key), None)
            if existing is not None:
                existing["count"] = int(existing.get("count") or 1) + 1
                existing["message"] = event.message
                existing["fields"] = dict(event.fields)
                existing["attempts"] = int(existing.get("attempts") or 0) + 1
                existing["_next_at"] = now + (
                    RETRY_KEY_SECONDS if key_event
                    else self._backoff_seconds(existing["attempts"]))
                record = existing
            else:
                record = self._event_to_record(event)
                record["_next_at"] = now + (
                    RETRY_KEY_SECONDS if key_event
                    else self._backoff_seconds(0))
                self._pending.append(record)
                while len(self._pending) > PENDING_MAX:
                    self._pending.pop(0)
                    logger.warning(
                        "notify: 待发队列超过上限 %d，丢弃最旧一条", PENDING_MAX)
            self._persist_pending()
            size = len(self._pending)
        logger.warning(
            "notify: 飞书限频（%s），告警已转入本地待发队列并退避重试"
            "（gate=%s，关键=%s，待发 %d 条）",
            RATE_LIMIT_CODE, event.gate or event.title, key_event, size)

    def _retry_due_pending(self) -> None:
        """重投所有到期的待发告警（发送线程内串行调用）。"""
        now = self._mono()
        with self._pending_lock:
            due = [r for r in self._pending if float(r.get("_next_at") or 0) <= now]
        for record in due:
            self._retry_one(record)

    def _retry_one(self, record: dict) -> None:
        event = self._record_to_event(record)
        cfg = self.config()
        if not cfg.usable:
            return
        delivered, errors = self._deliver(cfg, event)
        now = self._mono()
        key_event = is_key_event(event.level, event.gate, event.title)
        with self._pending_lock:
            if delivered or not errors:
                self._pending = [r for r in self._pending if r is not record]
                self._persist_pending()
                if delivered and int(record.get("count") or 1) > 1:
                    logger.info(
                        "notify: 限频积压已补发（合并 %d 条）gate=%s",
                        record.get("count"), event.gate or event.title)
                return
            record["attempts"] = int(record.get("attempts") or 0) + 1
            if is_rate_limited(errors[0]):
                record["_next_at"] = now + (
                    RETRY_KEY_SECONDS if key_event
                    else self._backoff_seconds(record["attempts"]))
            else:
                # 限频已解除但其它原因仍失败：继续退避，并按既有节流写日志
                record["_next_at"] = now + self._backoff_seconds(record["attempts"])
            self._persist_pending()
        if not is_rate_limited(errors[0]):
            self._log_fail(cfg, errors[0])

    def _load_pending(self) -> None:
        """进程启动时从磁盘恢复待发队列（只读；文件不存在/损坏则忽略）。"""
        with self._pending_lock:
            if self._pending_loaded:
                return
            self._pending_loaded = True
            path = self._pending_file()
            if path is None or not path.exists():
                return
            loaded: List[dict] = []
            try:
                text = path.read_text(encoding="utf-8")
            except OSError:
                logger.warning("notify: 待发队列读取失败（忽略）", exc_info=True)
                return
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(raw, dict) or not raw.get("title"):
                    continue
                try:
                    rec = self._event_to_record(
                        self._record_to_event(raw),
                        count=int(raw.get("count") or 1),
                        attempts=int(raw.get("attempts") or 0),
                    )
                except (TypeError, ValueError):
                    continue
                # 启动即尝试重投（上次进程留下的退避时间点不跨进程沿用）
                rec["_next_at"] = 0.0
                loaded.append(rec)
            self._pending = loaded[:PENDING_MAX]
            if self._pending:
                logger.info("notify: 已恢复 %d 条限频遗留告警，开始重投",
                            len(self._pending))

    def _persist_pending(self) -> None:
        """待发队列整体落盘（原子替换）；失败只告警，内存重试继续。"""
        path = self._pending_file()
        if path is None:
            return
        rows = [{k: v for k, v in r.items() if not k.startswith("_")}
                for r in self._pending]
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            if not rows:
                if path.exists():
                    path.unlink()
                return
            tmp = path.with_name(path.name + ".tmp")
            text = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
            tmp.write_text(text, encoding="utf-8")
            os.replace(tmp, path)
        except OSError:
            logger.warning("notify: 待发队列落盘失败（内存重试继续）", exc_info=True)

    def _log_fail(self, cfg: NotifyConfig, exc: BaseException) -> None:
        now = self._mono()
        if now - self._last_fail_log >= FAIL_LOG_INTERVAL_SECONDS:
            logger.warning(
                "notify: 告警外发失败（%s）；已节流日志 %d 秒，交易主链路不受影响",
                _sanitize(str(exc), cfg), int(FAIL_LOG_INTERVAL_SECONDS))
            # 先写日志再记时间戳：测试据此等待时不会读到「时间戳已更新、
            # 日志记录尚未入 handler」的中间态（发送线程内串行调用，无并发影响）。
            self._last_fail_log = now


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
