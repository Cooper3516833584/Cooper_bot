from __future__ import annotations

from types import SimpleNamespace

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
