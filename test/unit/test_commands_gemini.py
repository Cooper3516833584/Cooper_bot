from __future__ import annotations

from types import SimpleNamespace

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


def test_private_ai_chat_accepts_plain_text() -> None:
    ctx = SimpleNamespace(scene="private_friend")
    evt = {"message": [{"type": "text", "data": {"text": "你好"}}], "raw_message": "你好"}

    assert commands._extract_ai_chat_input(ctx, evt, "你好", "Cooper_bot") == "你好"


def test_private_ai_chat_gemini_prefix() -> None:
    ctx = SimpleNamespace(scene="private_friend")
    evt = {"message": [{"type": "text", "data": {"text": "g查一下"}}], "raw_message": "g查一下"}

    ai_input = commands._extract_ai_chat_input(ctx, evt, "g查一下", "Cooper_bot")
    backend, text = commands._split_ai_chat_backend(ai_input or "")

    assert backend == "gemini"
    assert text == "查一下"


def test_private_ai_chat_keeps_leading_cg_as_default_text() -> None:
    ctx = SimpleNamespace(scene="private_friend")
    evt = {"message": [{"type": "text", "data": {"text": "Cg查一下"}}], "raw_message": "Cg查一下"}

    ai_input = commands._extract_ai_chat_input(ctx, evt, "Cg查一下", "Cooper_bot")
    backend, text = commands._split_ai_chat_backend(ai_input or "")

    assert backend == "default"
    assert text == "Cg查一下"


def test_private_ai_chat_ignores_image_message() -> None:
    ctx = SimpleNamespace(scene="private_friend")
    evt = {
        "message": [{"type": "image", "data": {"file": "a.jpg"}}],
        "raw_message": "[CQ:image,file=a.jpg]",
    }

    assert commands._extract_ai_chat_input(ctx, evt, "[CQ:image,file=a.jpg]", "Cooper_bot") is None


def test_parse_handin_create_parts_with_suffix() -> None:
    task_name, suffix, time_texts, err = commands._parse_handin_create_parts(
        "作业1 PDF 1.22 18:30 1.23 20:00"
    )

    assert err == ""
    assert task_name == "作业1"
    assert suffix == "pdf"
    assert time_texts == ["1.22 18:30", "1.23 20:00"]


def test_pending_handin_required_suffix_uses_batch_source_names() -> None:
    item = {"name": "batch.zip", "source_names": ["a.txt", "b.PDF"]}

    assert commands._pending_handin_matches_required_suffix(item, "pdf") is True
    assert commands._pending_handin_matches_required_suffix(item, "docx") is False
