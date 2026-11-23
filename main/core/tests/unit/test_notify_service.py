"""告警外发通道（飞书 Webhook）单测：**全部 mock，不真发**。

锁定的口径：
1. 默认关闭 / 无 webhook_url → ``notify()`` 返回 False，不建线程、不发请求；
2. ``min_level`` 过滤；
3. 静默期去重键 =（闸门, 代码, 交易日），窗口内不重复、窗口外可再发；
4. 文本 / 富文本 payload 形状；配置 secret 时带 timestamp+sign；
5. 发送失败（飞书 code≠0、网络异常）**不抛**，只写日志；
6. 队列满不阻塞调用方。
"""
import json
import logging
import queue
import time

import pytest

from core.services import notify_service as ns


# ---------------------------------------------------------------- 假发送端
class _FakeResp:
    def __init__(self, payload=None, status_code=200):
        self.status_code = status_code
        self._payload = payload if payload is not None else {"code": 0, "msg": "success"}

    def json(self):
        return self._payload


class _FakeClient:
    """httpx.Client 替身：记录 post 的 json 体。"""

    def __init__(self, payload=None, status_code=200, exc=None):
        self.calls = []
        self._payload = payload
        self._status_code = status_code
        self._exc = exc

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def post(self, url, json=None, **kw):
        if self._exc is not None:
            raise self._exc
        self.calls.append({"url": url, "json": json})
        return _FakeResp(self._payload, self._status_code)


def _cfg(**kw):
    base = dict(enabled=True, webhook_url="https://example.invalid/hook",
                secret="", min_level="warn", silence_seconds=300.0,
                timeout_seconds=5.0)
    base.update(kw)
    return ns.NotifyConfig(**base)


class _Recorder:
    """sender_factory 替身：把每条消息记下来（不真发）。"""

    def __init__(self, exc=None):
        self.sent = []
        self._exc = exc

    def __call__(self, cfg):
        return self

    def send_text(self, text):
        if self._exc is not None:
            raise self._exc
        self.sent.append(("text", text))
        return {"code": 0}

    def send_rich(self, title, lines):
        if self._exc is not None:
            raise self._exc
        self.sent.append(("post", title, list(lines)))
        return {"code": 0}


def _wait(predicate, timeout=3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


# ---------------------------------------------------------------- 配置解析
@pytest.fixture(autouse=True)
def _allow_real_config_parse(monkeypatch):
    """conftest 会把 TQ_NOTIFY_DISABLED 置 1；本文件要验证真实解析，先清掉。"""
    monkeypatch.delenv("TQ_NOTIFY_DISABLED", raising=False)


def test_env_kill_switch_forces_disabled(monkeypatch):
    monkeypatch.setenv("TQ_NOTIFY_DISABLED", "1")
    cfg = ns.from_config({"notify": {"enabled": True,
                                     "webhook_url": "https://example.invalid/h"}})
    assert cfg.enabled is False and cfg.usable is False


def test_from_config_defaults_closed_when_section_missing():
    cfg = ns.from_config({})
    assert cfg.enabled is False and cfg.usable is False
    assert cfg.min_level == ns.DEFAULT_MIN_LEVEL


def test_from_config_clamps_timeout_and_missing_url_is_unusable():
    cfg = ns.from_config({"notify": {
        "enabled": True, "webhook_url": "", "timeout_seconds": 60,
        "silence_seconds": 0, "min_level": "ERROR"}})
    assert cfg.enabled is True and cfg.usable is False  # 无地址＝关闭
    assert cfg.timeout_seconds <= 5.0
    assert cfg.silence_seconds == ns.DEFAULT_SILENCE_SECONDS  # <=0 回退默认
    assert cfg.min_level == "error"


def test_from_config_bad_section_does_not_raise():
    cfg = ns.from_config({"notify": "not-a-dict"})
    assert cfg.usable is False


# ---------------------------------------------------------------- 关闭/过滤
def test_notify_is_noop_when_disabled():
    rec = _Recorder()
    notifier = ns.Notifier(cfg=_cfg(enabled=False), sender_factory=rec)
    assert notifier.notify("critical", "t", "m") is False
    assert notifier._thread is None
    assert rec.sent == []


def test_min_level_filters_out_lower_levels():
    rec = _Recorder()
    notifier = ns.Notifier(cfg=_cfg(min_level="error"), sender_factory=rec)
    try:
        assert notifier.notify("warn", "t", "m") is False
        assert notifier.notify("info", "t", "m") is False
        assert notifier.notify("error", "t", "m") is True
        assert _wait(lambda: len(rec.sent) == 1)
    finally:
        notifier.close()


# ---------------------------------------------------------------- 去重
def test_silence_window_dedupes_same_gate_code_date():
    rec = _Recorder()
    notifier = ns.Notifier(cfg=_cfg(), sender_factory=rec)
    try:
        assert notifier.notify("error", "t", "m", gate="cash_diff",
                               code="000001.SZ", trade_date="2026-11-18") is True
        assert notifier.notify("error", "t", "m", gate="cash_diff",
                               code="000001.SZ", trade_date="2026-11-18") is False
        assert _wait(lambda: len(rec.sent) == 1)
    finally:
        notifier.close()


def test_silence_window_expires_and_different_code_passes():
    rec = _Recorder()
    clock = {"t": 1000.0}
    notifier = ns.Notifier(cfg=_cfg(), sender_factory=rec,
                           mono_fn=lambda: clock["t"])
    try:
        assert notifier.notify("error", "t", "m", gate="cash_diff",
                               code="A", trade_date="2026-11-18") is True
        assert notifier.notify("error", "t", "m", gate="cash_diff",
                               code="B", trade_date="2026-11-18") is True
        # 静默期内同键再次被拦
        clock["t"] += 10
        assert notifier.notify("error", "t", "m", gate="cash_diff",
                               code="A", trade_date="2026-11-18") is False
        # 超出静默期后可再发
        clock["t"] += 400
        assert notifier.notify("error", "t", "m", gate="cash_diff",
                               code="A", trade_date="2026-11-18") is True
        assert _wait(lambda: len(rec.sent) == 3)
    finally:
        notifier.close()


# ---------------------------------------------------------------- payload
def test_text_payload_shape():
    client = _FakeClient()
    sender = ns.FeishuSender("https://example.invalid/hook",
                             client_factory=lambda: client)
    sender.send_text("hello")
    body = client.calls[0]["json"]
    assert body == {"msg_type": "text", "content": {"text": "hello"}}


def test_rich_payload_shape():
    client = _FakeClient()
    sender = ns.FeishuSender("https://example.invalid/hook",
                             client_factory=lambda: client)
    sender.send_rich("标题", ["第一行", "第二行"])
    body = client.calls[0]["json"]
    post = body["content"]["post"]["zh_cn"]
    assert body["msg_type"] == "post"
    assert post["title"] == "标题"
    assert post["content"] == [[{"tag": "text", "text": "第一行"}],
                               [{"tag": "text", "text": "第二行"}]]


def test_secret_adds_timestamp_and_sign():
    client = _FakeClient()
    sender = ns.FeishuSender("https://example.invalid/hook", secret="s3cret",
                             client_factory=lambda: client)
    sender.send_text("x")
    body = client.calls[0]["json"]
    assert body["timestamp"] and body["sign"]
    # 签名 = base64(HMAC-SHA256(key=f"{ts}\n{secret}", msg=""))
    import base64
    import hashlib
    import hmac
    expected = base64.b64encode(hmac.new(
        ("%s\ns3cret" % body["timestamp"]).encode(), b"",
        digestmod=hashlib.sha256).digest()).decode()
    assert body["sign"] == expected


# ---------------------------------------------------------------- 失败不影响调用方
def test_feishu_nonzero_code_raises_notify_error():
    client = _FakeClient(payload={"code": 19021, "msg": "sign match fail"})
    sender = ns.FeishuSender("https://example.invalid/hook",
                             client_factory=lambda: client)
    with pytest.raises(ns.NotifyError):
        sender.send_text("x")


def test_send_failure_never_propagates_and_throttles_log(caplog):
    rec = _Recorder(exc=RuntimeError("boom"))
    notifier = ns.Notifier(cfg=_cfg(), sender_factory=rec)
    try:
        with caplog.at_level(logging.WARNING):
            assert notifier.notify("error", "t", "m", gate="g1") is True
            assert notifier.notify("error", "t", "m", gate="g2") is True
            assert _wait(lambda: notifier._last_fail_log > 0)
            time.sleep(0.1)
        fails = [r for r in caplog.records if "告警外发失败" in r.getMessage()]
        assert len(fails) == 1  # 600s 节流：两条失败只打一条
    finally:
        notifier.close()


def test_network_exception_is_wrapped_as_notify_error():
    client = _FakeClient(exc=TimeoutError("timed out"))
    sender = ns.FeishuSender("https://example.invalid/hook",
                             client_factory=lambda: client)
    with pytest.raises(ns.NotifyError):
        sender.send_text("x")


# ---------------------------------------------------------------- 敏感信息脱敏
_FAKE_SECRET = "FAKEsecretABCDEFGH1234"       # 测试用假值（真实 secret 不入库/不入 git）
_FAKE_URL = "https://example.invalid/hook/abcdef123456"


def test_mask_sensitive_keeps_only_tail():
    masked = ns.mask_sensitive(_FAKE_SECRET)
    assert masked.startswith("***") and masked.endswith("1234")
    assert _FAKE_SECRET[:-4] not in masked
    assert ns.mask_sensitive("") == "—"


def test_failure_log_masks_webhook_and_secret(caplog):
    rec = _Recorder(exc=RuntimeError("connect failed to %s (secret %s)"
                                     % (_FAKE_URL, _FAKE_SECRET)))
    notifier = ns.Notifier(cfg=_cfg(webhook_url=_FAKE_URL, secret=_FAKE_SECRET),
                           sender_factory=rec)
    try:
        with caplog.at_level(logging.WARNING):
            assert notifier.notify("error", "t", "m", gate="leak") is True
            assert _wait(lambda: notifier._last_fail_log > 0)
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert _FAKE_SECRET not in text
        assert _FAKE_URL not in text
        assert "告警外发失败" in text
    finally:
        notifier.close()


# ---------------------------------------------------------------- 队列满
def test_full_queue_drops_without_raising():
    rec = _Recorder()
    notifier = ns.Notifier(cfg=_cfg(), sender_factory=rec)
    # 塞满队列且不启动发送线程：把 _ensure_thread 变成 no-op 以模拟消费者停摆
    notifier._ensure_thread = lambda: None
    for i in range(ns.QUEUE_MAX):
        notifier._queue.put_nowait(ns.NotifyEvent("warn", "t", str(i)))
    assert notifier.notify("error", "t", "m", gate="overflow") is False
    assert notifier._dropped == 1


# ---------------------------------------------------------------- 模块级入口
def test_module_level_notify_swallows_errors(monkeypatch):
    class _Broken:
        def notify(self, *a, **kw):
            raise RuntimeError("boom")

    monkeypatch.setattr(ns, "get_notifier", lambda: _Broken())
    assert ns.notify("error", "t", "m") is False


def test_module_level_notify_uses_singleton(monkeypatch):
    rec = _Recorder()
    notifier = ns.Notifier(cfg=_cfg(), sender_factory=rec)
    ns.reset_notifier(notifier)
    try:
        assert ns.notify("error", "t", "m", gate="g") is True
        assert _wait(lambda: len(rec.sent) == 1)
    finally:
        notifier.close()
        ns.reset_notifier(None)


def test_queue_is_non_blocking_type():
    assert isinstance(ns.Notifier(cfg=_cfg())._queue, queue.Queue)
    json.dumps({"ok": True})  # 保证 stdlib json 导入不被误删


# ================================================================ 批次21 L1
# 多渠道（企微 / 邮件 / 本地提示音）：默认关闭，未配置时与旧行为逐字一致。
def test_from_config_defaults_keep_single_channel_semantics():
    """只配飞书（旧配置）→ 解析结果与旧实现等价，可选渠道全部关闭。"""
    cfg = ns.from_config({"notify": {
        "enabled": True, "webhook_url": "https://example.invalid/h",
        "secret": "s", "min_level": "warn"}})
    assert cfg.usable is True
    assert cfg.wecom_webhook_url == ""
    assert cfg.email_host == "" and cfg.email_to == ()
    assert cfg.sound_enabled is False
    assert cfg.sound_usable is False


def test_from_config_parses_optional_channels(monkeypatch):
    monkeypatch.delenv("TQ_NOTIFY_EMAIL_PASSWORD", raising=False)
    cfg = ns.from_config({"notify": {
        "enabled": True,  # 没有任何 webhook，仅企微+邮件也可用
        "wecom": {"webhook_url": "https://example.invalid/wecom"},
        "email": {
            "smtp_host": "smtp.example.invalid", "smtp_port": "587",
            "username": "alarm@example.invalid", "password": "pw",
            "from": "alarm@example.invalid",
            "to": "a@example.invalid; b@example.invalid",
            "use_ssl": False,
        },
        "sound": {"enabled": True, "path": "C:/alarm.wav"},
    }})
    assert cfg.usable is True
    assert cfg.wecom_usable is True and cfg.feishu_usable is False
    assert cfg.email_port == 587 and cfg.email_use_ssl is False
    assert cfg.email_to == ("a@example.invalid", "b@example.invalid")
    assert cfg.sound_enabled is True and cfg.sound_path == "C:/alarm.wav"
    assert cfg.email_usable is True


def test_from_config_email_password_env_overrides(monkeypatch):
    monkeypatch.setenv("TQ_NOTIFY_EMAIL_PASSWORD", "from-env")
    cfg = ns.from_config({"notify": {"enabled": True, "email": {
        "smtp_host": "h", "to": ["a@b.c"], "password": "from-yaml"}}})
    assert cfg.email_password == "from-env"


def test_from_config_invalid_port_falls_back(monkeypatch):
    monkeypatch.delenv("TQ_NOTIFY_EMAIL_PASSWORD", raising=False)
    cfg = ns.from_config({"notify": {"enabled": True, "email": {
        "smtp_host": "h", "smtp_port": "not-a-port", "to": ["a@b.c"]}}})
    assert cfg.email_port == 465


class _CountRecorder(_Recorder):
    """多渠道各自一个实例：记录各自收到几条。"""


def _multi_cfg(**kw):
    base = dict(enabled=True, webhook_url="https://example.invalid/feishu",
                wecom_webhook_url="https://example.invalid/wecom",
                email_host="smtp.example.invalid", email_to=("a@b.c",),
                sound_enabled=True)
    base.update(kw)
    return ns.NotifyConfig(**base)


def test_notifier_delivers_to_every_configured_channel():
    feishu, wecom, email, sound = (_CountRecorder(), _CountRecorder(),
                                   _CountRecorder(), _CountRecorder())
    notifier = ns.Notifier(cfg=_multi_cfg(), sender_factory=feishu,
                           wecom_factory=wecom, email_factory=email,
                           sound_factory=sound)
    try:
        assert notifier.notify("error", "t", "m", gate="multi") is True
        assert _wait(lambda: all(len(r.sent) == 1
                                 for r in (feishu, wecom, email, sound)))
    finally:
        notifier.close()


def test_one_channel_failure_does_not_block_others():
    feishu = _CountRecorder(exc=RuntimeError("feishu down"))
    wecom = _CountRecorder()
    notifier = ns.Notifier(
        cfg=_multi_cfg(email_host="", sound_enabled=False),
        sender_factory=feishu, wecom_factory=wecom)
    try:
        assert notifier.notify("error", "t", "m", gate="partial") is True
        assert _wait(lambda: len(wecom.sent) == 1)
        # 有渠道成功 → 不写失败日志
        assert notifier._last_fail_log == 0.0
    finally:
        notifier.close()


def test_all_channels_failure_still_throttles_log(caplog):
    feishu = _CountRecorder(exc=RuntimeError("boom1"))
    wecom = _CountRecorder(exc=RuntimeError("boom2"))
    notifier = ns.Notifier(
        cfg=_multi_cfg(email_host="", sound_enabled=False),
        sender_factory=feishu, wecom_factory=wecom)
    try:
        with caplog.at_level(logging.WARNING):
            assert notifier.notify("error", "t", "m", gate="all-fail") is True
            assert _wait(lambda: any(
                "告警外发失败" in r.getMessage() for r in caplog.records))
        fails = [r for r in caplog.records if "告警外发失败" in r.getMessage()]
        assert len(fails) == 1
    finally:
        notifier.close()


def test_unconfigured_optional_channels_are_not_built():
    """只配飞书时不构造企微/邮件/提示音发送器（与旧行为一致）。"""
    feishu = _CountRecorder()
    calls = []

    def _record(kind):
        def _factory(cfg):
            calls.append(kind)
            return _CountRecorder()
        return _factory

    notifier = ns.Notifier(cfg=_cfg(), sender_factory=feishu,
                           wecom_factory=_record("wecom"),
                           email_factory=_record("email"),
                           sound_factory=_record("sound"))
    try:
        assert notifier.notify("error", "t", "m", gate="only-feishu") is True
        assert _wait(lambda: len(feishu.sent) == 1)
        assert calls == []
    finally:
        notifier.close()


# ---------------------------------------------------------------- 企微 payload
def test_wecom_text_and_markdown_payload():
    client = _FakeClient(payload={"errcode": 0, "errmsg": "ok"})
    sender = ns.WecomSender("https://example.invalid/wecom",
                            client_factory=lambda: client)
    sender.send_text("hello")
    assert client.calls[0]["json"] == {
        "msgtype": "text", "text": {"content": "hello"}}
    sender.send_rich("标题", ["第一行", "第二行"])
    body = client.calls[1]["json"]
    assert body["msgtype"] == "markdown"
    assert "**标题**" in body["markdown"]["content"]
    assert "> 第一行" in body["markdown"]["content"]


def test_wecom_errcode_raises_notify_error():
    client = _FakeClient(payload={"errcode": 93000, "errmsg": "invalid webhook"})
    sender = ns.WecomSender("https://example.invalid/wecom",
                            client_factory=lambda: client)
    with pytest.raises(ns.NotifyError):
        sender.send_text("x")


# ---------------------------------------------------------------- 邮件
class _FakeSMTP:
    def __init__(self):
        self.tls = False
        self.login_args = None
        self.messages = []
        self.quit_called = False

    def starttls(self):
        self.tls = True

    def login(self, user, password):
        self.login_args = (user, password)

    def send_message(self, msg):
        self.messages.append(msg)

    def quit(self):
        self.quit_called = True


def test_email_sender_builds_and_sends_message():
    smtp = _FakeSMTP()
    sender = ns.EmailSender(
        "smtp.example.invalid", 465, "alarm@example.invalid", "pw",
        ("a@b.c", "d@e.f"), "alarm@example.invalid", True,
        smtp_factory=lambda h, p, t: smtp)
    sender.send_text("第一行\n第二行")
    assert smtp.login_args == ("alarm@example.invalid", "pw")
    assert smtp.quit_called is True and smtp.tls is False  # SSL 模式不走 starttls
    msg = smtp.messages[0]
    assert msg["To"] == "a@b.c, d@e.f"
    assert msg["From"] == "alarm@example.invalid"
    assert "第一行" in msg.get_content()


def test_email_sender_starttls_when_not_ssl():
    smtp = _FakeSMTP()
    sender = ns.EmailSender("h", 587, "", "", ("a@b.c",), use_ssl=False,
                            smtp_factory=lambda h, p, t: smtp)
    sender.send_rich("标题", ["行1"])
    assert smtp.tls is True
    assert smtp.login_args is None  # 未配置用户不登录


def test_email_sender_wraps_failure_as_notify_error():
    def _boom(h, p, t):
        raise OSError("connection refused")

    sender = ns.EmailSender("h", 465, to=("a@b.c",), smtp_factory=_boom)
    with pytest.raises(ns.NotifyError):
        sender.send_text("x")


# ---------------------------------------------------------------- 提示音
def test_sound_sender_invokes_player_with_path():
    played = []
    sender = ns.SoundSender("C:/alarm.wav", player=played.append)
    sender.send_text("x")
    sender.send_rich("t", ["line"])
    assert played == ["C:/alarm.wav", "C:/alarm.wav"]


def test_sound_sender_propagates_player_failure_as_notify_error():
    def _boom(_path):
        raise ns.NotifyError("当前平台不支持本地提示音")

    sender = ns.SoundSender(player=_boom)
    with pytest.raises(ns.NotifyError):
        sender.send_text("x")


# ---------------------------------------------------------------- 脱敏扩展
_FAKE_WECOM = "https://example.invalid/wecom/abcdef123456"


# ================================================================ P2-10 限频
_RATE_LIMIT_EXC = ns.NotifyError("飞书返回 code=11232 msg=frequency limited")


def _pending_rows(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()]


def test_rate_limited_event_is_parked_then_retried(tmp_path):
    """11232 不再丢弃：落待发队列，退避到点后重投成功并清理文件。"""
    pending = tmp_path / "notify_pending.jsonl"
    clock = [1000.0]

    class _Flaky(_Recorder):
        def __init__(self):
            super().__init__()
            self.fail_times = 1

        def send_text(self, text):
            if self.fail_times > 0:
                self.fail_times -= 1
                raise _RATE_LIMIT_EXC
            return super().send_text(text)

    sender = _Flaky()
    notifier = ns.Notifier(cfg=_cfg(), sender_factory=sender,
                           mono_fn=lambda: clock[0], pending_path=pending)
    try:
        assert notifier.notify("warn", "行情停滞", "x",
                               gate="market_data_stalled", code="1") is True
        assert _wait(lambda: pending.exists() and _pending_rows(pending))
        notifier.close()  # 停掉后台线程，改由本用例手动驱动重投（避免并发重投）
        rows = _pending_rows(pending)
        assert rows[0]["gate"] == "market_data_stalled"
        assert notifier._pending and notifier._pending[0]["_next_at"] >= clock[0]
        # 退避未到点：不重投
        clock[0] += 1.0
        notifier._retry_due_pending()
        assert notifier._pending
        # 退避到点：重投成功并清空
        clock[0] += ns.RETRY_BASE_SECONDS
        notifier._retry_due_pending()
        assert notifier._pending == []
        assert not pending.exists()
        assert len(sender.sent) == 1
    finally:
        notifier.close()


def test_rate_limited_key_events_merge_and_downshift(tmp_path):
    """关键事件（卖出/风控/熔断）限频：按去重键合并计数、按关键间隔降频重投。"""
    pending = tmp_path / "notify_pending.jsonl"
    clock = [2000.0]
    sender = _Recorder(exc=_RATE_LIMIT_EXC)
    notifier = ns.Notifier(cfg=_cfg(), sender_factory=sender,
                           mono_fn=lambda: clock[0], pending_path=pending)
    try:
        ev = ns.NotifyEvent("critical", "风控熔断", "触发熔断",
                            gate="breaker", code="000001")
        notifier._send_one(ev)
        clock[0] += 10.0
        notifier._send_one(ev)
        rows = _pending_rows(pending)
        assert len(rows) == 1
        assert rows[0]["count"] == 2
        assert notifier._pending[0]["_next_at"] == clock[0] + ns.RETRY_KEY_SECONDS
    finally:
        notifier.close()


def test_pending_queue_resumes_after_restart(tmp_path):
    """进程重启后由 start() 恢复落盘队列并继续投递。"""
    pending = tmp_path / "notify_pending.jsonl"
    pending.write_text(json.dumps({
        "level": "error", "title": "卖出未成交", "message": "重投我",
        "fields": {"代码": "600000"}, "gate": "sell", "code": "600000",
        "trade_date": "2026-11-23", "count": 3, "attempts": 1,
        "created_at": 1.0}, ensure_ascii=False) + "\n", encoding="utf-8")
    sender = _Recorder()
    notifier = ns.Notifier(cfg=_cfg(), sender_factory=sender,
                           pending_path=pending)
    try:
        notifier.start()
        assert _wait(lambda: len(sender.sent) == 1)
        assert _wait(lambda: not pending.exists())
        assert notifier._pending == []
    finally:
        notifier.close()


def test_transient_failure_without_rate_limit_stays_log_only(tmp_path, caplog):
    """非 11232 失败保持旧口径：不落待发队列、只节流写日志。"""
    pending = tmp_path / "notify_pending.jsonl"
    sender = _Recorder(exc=RuntimeError("connection refused"))
    notifier = ns.Notifier(cfg=_cfg(), sender_factory=sender,
                           pending_path=pending)
    try:
        with caplog.at_level(logging.WARNING):
            assert notifier.notify("warn", "t", "m", gate="net") is True
            assert _wait(lambda: notifier._last_fail_log > 0)
        assert notifier._pending == []
        assert not pending.exists()
    finally:
        notifier.close()


def test_failure_log_masks_wecom_url_and_email_password(caplog):
    leaker = _CountRecorder(exc=RuntimeError(
        "wecom %s email pw %s" % (_FAKE_WECOM, "EmAilPw9876")))
    notifier = ns.Notifier(
        cfg=ns.NotifyConfig(enabled=True, wecom_webhook_url=_FAKE_WECOM,
                            email_password="EmAilPw9876"),
        wecom_factory=leaker)
    try:
        with caplog.at_level(logging.WARNING):
            assert notifier.notify("error", "t", "m", gate="leak2") is True
            assert _wait(lambda: notifier._last_fail_log > 0)
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert _FAKE_WECOM not in text and "EmAilPw9876" not in text
    finally:
        notifier.close()


def test_default_pending_path_follows_runtime_data_dir(monkeypatch, tmp_path):
    """待发队列与体检游标/台账同口径：落 runtime.data_dir()，不再各自推算。"""
    from core import runtime

    monkeypatch.delenv("TQ_NOTIFY_PENDING_DISABLED", raising=False)
    assert ns.default_pending_path() == runtime.data_dir() / "notify_pending.jsonl"

    monkeypatch.setattr(ns, "data_dir", lambda: tmp_path)
    assert ns.default_pending_path() == tmp_path / "notify_pending.jsonl"

    monkeypatch.setenv("TQ_NOTIFY_PENDING_DISABLED", "1")
    assert ns.default_pending_path() is None
