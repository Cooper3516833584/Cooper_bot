from __future__ import annotations

import json
from email.message import EmailMessage
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import cooper_bot.modules.email_notify.mail_notify as mail_notify
from cooper_bot.modules.ai.aisvc import AIService
from cooper_bot.modules.email_notify.mail_notify import EmailNotifyService, FetchBatch, ImapAccount, ParsedMail


class _Log:
    def __init__(self) -> None:
        self.infos: list[str] = []
        self.warnings: list[str] = []

    def info(self, text: str) -> None:
        self.infos.append(str(text))

    def warning(self, text: str) -> None:
        self.warnings.append(str(text))


class _AI:
    def __init__(self, result: dict | None = None, error: Exception | None = None) -> None:
        self.result = result or {"category": "normal", "notify": True, "summary": "完整摘要"}
        self.error = error
        self.calls: list[dict] = []

    async def classify_email(self, **kwargs) -> dict:
        self.calls.append(dict(kwargs))
        if self.error is not None:
            raise self.error
        return dict(self.result)


def _account() -> ImapAccount:
    return ImapAccount(
        account_id="netease",
        label="网易邮箱",
        host="imap.163.com",
        port=993,
        username="user@163.com",
        password="test-only",
    )


def _service(tmp_path, ai=None) -> EmailNotifyService:
    return EmailNotifyService(
        _Log(),
        ai or _AI(),
        enabled=True,
        state_path=tmp_path / "email_notify_state.json",
        accounts=[_account()],
    )


def test_loads_multiple_imap_accounts_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("MAIL_IMAP_ACCOUNTS", "netease,gmail")
    monkeypatch.setenv("MAIL_IMAP_NETEASE_HOST", "imap.163.com")
    monkeypatch.setenv("MAIL_IMAP_NETEASE_USER", "one@163.com")
    monkeypatch.setenv("MAIL_IMAP_NETEASE_PASSWORD", "auth-code")
    monkeypatch.setenv("MAIL_IMAP_GMAIL_HOST", "imap.gmail.com")
    monkeypatch.setenv("MAIL_IMAP_GMAIL_USER", "two@gmail.com")
    monkeypatch.setenv("MAIL_IMAP_GMAIL_PASSWORD", "app-password")

    accounts = EmailNotifyService._load_accounts_from_env()

    assert [item.account_id for item in accounts] == ["netease", "gmail"]
    assert accounts[0].host == "imap.163.com"
    assert accounts[1].host == "imap.gmail.com"


def test_netease_connection_sends_required_client_id(tmp_path) -> None:
    calls: list[tuple[str, str]] = []

    class _Client:
        def _simple_command(self, command: str, value: str):
            calls.append((command, value))
            return "OK", [b"success"]

    _service(tmp_path)._send_netease_client_id(_Client(), _account())

    assert calls and calls[0][0] == "ID"
    assert "Cooper_bot" in calls[0][1]


def test_parse_message_extracts_body_and_attachment(tmp_path) -> None:
    message = EmailMessage()
    message["From"] = "=?utf-8?b?5pWZ5Yqh5aSE?= <office@example.edu>"
    message["To"] = "user@example.com"
    message["Subject"] = "=?utf-8?b?5a6e6aqM5oql5ZGK5o+Q5Lqk?="
    message["Date"] = "Tue, 18 Aug 2026 14:30:00 +0800"
    message.set_content("请在明天下午五点前提交实验报告，模板见附件。")
    message.add_attachment(b"test", maintype="application", subtype="pdf", filename="template.pdf")

    parsed = _service(tmp_path)._parse_message(12, message.as_bytes())

    assert parsed.uid == 12
    assert "教务处" in parsed.sender
    assert "实验报告提交" in parsed.subject
    assert "下午五点" in parsed.body
    assert parsed.has_attachment is True
    assert parsed.attachment_names == ("template.pdf",)


def test_parse_message_uses_html_when_plain_text_is_missing(tmp_path) -> None:
    raw = (
        b"From: service@example.com\r\n"
        b"Subject: Verification\r\n"
        b"Content-Type: text/html; charset=utf-8\r\n\r\n"
        b"<html><style>.x{display:none}</style><body><p>Code: <b>123456</b></p></body></html>"
    )

    parsed = _service(tmp_path)._parse_message(13, raw)

    assert "Code:" in parsed.body
    assert "123456" in parsed.body
    assert "display:none" not in parsed.body


@pytest.mark.asyncio
async def test_normal_email_sends_detailed_text_and_attachment_hint(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(mail_notify, "ADMIN_USERS", {900001})
    ai = _AI({"category": "normal", "notify": True, "summary": "需在明天前处理。&#91;原文本&#93;"})
    service = _service(tmp_path, ai)
    api = SimpleNamespace(send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}))
    message = ParsedMail(
        uid=14,
        sender="官方服务 <notice@example.com>",
        subject="账号安全提醒",
        received_at="2026-08-18 15:00:00",
        body="检测到新的登录操作。",
        has_attachment=True,
        attachment_names=("说明.pdf",),
    )

    assert await service._process_message(api, _account(), message) is True

    api.send_private_msg.assert_awaited_once()
    user_id, text = api.send_private_msg.await_args.args
    assert user_id == 900001
    assert "账号安全提醒" in text
    assert "附件：有（说明.pdf）" in text
    assert "邮件摘要：" in text
    assert "需在明天前处理" in text


@pytest.mark.asyncio
async def test_spam_email_is_suppressed(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(mail_notify, "ADMIN_USERS", {900001})
    service = _service(tmp_path, _AI({"category": "spam", "notify": False, "summary": ""}))
    api = SimpleNamespace(send_private_msg=AsyncMock())
    message = ParsedMail(15, "spam@example.com", "中奖", "2026-08-18", "点击领取", False)

    assert await service._process_message(api, _account(), message) is True
    api.send_private_msg.assert_not_awaited()


@pytest.mark.asyncio
async def test_model_failure_still_notifies_with_body_fallback(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(mail_notify, "ADMIN_USERS", {900001})
    service = _service(tmp_path, _AI(error=RuntimeError("model offline")))
    api = SimpleNamespace(send_private_msg=AsyncMock(return_value={"status": "ok", "retcode": 0}))
    message = ParsedMail(16, "service@example.com", "验证码", "2026-08-18", "验证码是 654321。", False)

    assert await service._process_message(api, _account(), message) is True
    assert "654321" in api.send_private_msg.await_args.args[1]


@pytest.mark.asyncio
async def test_first_poll_only_initializes_uid_cursor(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(mail_notify, "ADMIN_USERS", {900001})
    service = _service(tmp_path)
    monkeypatch.setattr(
        service,
        "_fetch_batch_sync",
        lambda _account, _state: FetchBatch("777", 42, (), reset_cursor=True),
    )
    api = SimpleNamespace(send_private_msg=AsyncMock())

    await service._poll_account_once(api, _account())

    saved = json.loads(service.state_path.read_text(encoding="utf-8"))
    assert saved["accounts"]["netease"]["uidvalidity"] == "777"
    assert saved["accounts"]["netease"]["last_uid"] == 42
    api.send_private_msg.assert_not_awaited()


@pytest.mark.asyncio
async def test_deepseek_email_classifier_uses_json_mode(monkeypatch) -> None:
    service = AIService(_Log())
    service.deepseek_base_url = "https://deepseek.example/v1"
    service.deepseek_api_key = "test-only"
    captured: dict = {}

    def _fake_post(url: str, payload: dict, api_key: str, timeout: float) -> dict:
        captured.update(url=url, payload=payload, api_key=api_key, timeout=timeout)
        return {
            "choices": [
                {
                    "message": {
                        "content": '{"category":"normal","notify":true,"summary":"验证码为 123456，请勿泄露。"}'
                    }
                }
            ]
        }

    monkeypatch.setattr(service, "_post_json", _fake_post)

    result = await service.classify_email(
        sender="security@example.com",
        subject="验证码",
        body="本次验证码为 123456。",
        has_attachment=False,
    )

    assert result["notify"] is True
    assert "123456" in result["summary"]
    assert captured["payload"]["model"] == service.chat_model
    assert captured["payload"]["response_format"] == {"type": "json_object"}
    assert captured["timeout"] == 45.0
