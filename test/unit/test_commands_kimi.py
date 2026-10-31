from __future__ import annotations

from types import SimpleNamespace

import pytest

import cooper_bot.commands.commands as commands


def test_split_ai_chat_backend_keeps_legacy_prefixes_as_text() -> None:
    for text in ("g 帮我联网查一下", "c 帮我联网查一下", "G帮我联网查一下", "C帮我联网查一下"):
        backend, clean = commands._split_ai_chat_backend(text)
        assert backend == "default"
        assert clean == text


def test_private_ai_chat_accepts_plain_and_legacy_prefix_text() -> None:
    ctx = SimpleNamespace(scene="private_friend")
    evt = {"message": [{"type": "text", "data": {"text": "g查一下"}}], "raw_message": "g查一下"}

    ai_input = commands._extract_ai_chat_input(ctx, evt, "g查一下", "Cooper_bot")
    backend, text = commands._split_ai_chat_backend(ai_input or "")

    assert backend == "default"
    assert text == "g查一下"


def test_private_ai_chat_ignores_image_message() -> None:
    ctx = SimpleNamespace(scene="private_friend")
    evt = {
        "message": [{"type": "image", "data": {"file": "a.jpg"}}],
        "raw_message": "[CQ:image,file=a.jpg]",
    }

    assert commands._extract_ai_chat_input(ctx, evt, "[CQ:image,file=a.jpg]", "Cooper_bot") is None


@pytest.mark.parametrize("scene", ["group", "private_friend", "private_group", "private_stranger"])
@pytest.mark.parametrize("admin_source", ["allowlist", "permissions"])
def test_computer_access_depends_on_personal_admin_account(monkeypatch, scene, admin_source) -> None:
    monkeypatch.setattr(commands, "ADMIN_USERS", {10001} if admin_source == "allowlist" else set())
    perm = SimpleNamespace(get_level=lambda _uid: 3 if admin_source == "permissions" else 0)
    ctx = SimpleNamespace(scene=scene, user_id=10001, level=0)

    assert commands._ai_chat_allows_kimi_computer(ctx, perm) is True


@pytest.mark.parametrize("user_id", [0, -1, "invalid", None, 10002])
def test_context_admin_level_does_not_grant_computer_access(monkeypatch, user_id) -> None:
    monkeypatch.setattr(commands, "ADMIN_USERS", {10001})
    ctx = SimpleNamespace(scene="group", user_id=user_id, level=3)
    perm = SimpleNamespace(get_level=lambda _uid: 1)

    assert commands._ai_chat_allows_kimi_computer(ctx, perm) is False


def test_computer_access_rejects_unavailable_personal_permissions(monkeypatch) -> None:
    monkeypatch.setattr(commands, "ADMIN_USERS", set())
    ctx = SimpleNamespace(scene="group", user_id=10001, level=3)

    def unavailable(_uid):
        raise RuntimeError("permissions unavailable")

    assert commands._ai_chat_allows_kimi_computer(ctx) is False
    assert commands._ai_chat_allows_kimi_computer(ctx, SimpleNamespace(get_level=unavailable)) is False
