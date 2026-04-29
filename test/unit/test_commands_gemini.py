from __future__ import annotations

import commands


def test_split_ai_chat_backend_gemini_prefix() -> None:
    backend, text = commands._split_ai_chat_backend("g 帮我联网查一下")
    assert backend == "gemini"
    assert text == "帮我联网查一下"


def test_split_ai_chat_backend_gemini_prefix_without_space_and_uppercase() -> None:
    backend, text = commands._split_ai_chat_backend("G帮我联网查一下")
    assert backend == "gemini"
    assert text == "帮我联网查一下"


def test_split_ai_chat_backend_default_prefix() -> None:
    backend, text = commands._split_ai_chat_backend("正常聊天内容")
    assert backend == "default"
    assert text == "正常聊天内容"
