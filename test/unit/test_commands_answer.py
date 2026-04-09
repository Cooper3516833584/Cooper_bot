from __future__ import annotations

import commands


NI_HAO = "\u4f60\u597d"
NIN_HAO = "\u60a8\u597d"
LINE_1 = "\u7b2c\u4e00\u884c"
LINE_2 = "\u7b2c\u4e8c\u884c"
EXTRA = "\u8865\u5145\u4e00\u6761"
LONG_MSG_1 = "\u8001\u5e08\u4f60\u597d\uff0c\u6211\u60f3\u95ee\u4e2a\u95ee\u9898"
LONG_MSG_2 = "\u60a8\u597d\uff0c\u8bf7\u95ee\u600e\u4e48\u7528"
LONG_MSG_3 = "\u5927\u5bb6\u597d\uff0c\u987a\u4fbf\u8bf4\u4e00\u53e5\u4f60\u597d\u5440\uff01"
NI_HAO_YA = "\u4f60\u597d\u5440"


def test_parse_answer_txt_supports_synonyms_multiline_and_multi_message() -> None:
    content = (
        f"q:{NI_HAO}\n"
        f"q:{NIN_HAO}\n"
        "a:|\n"
        f"  {LINE_1}\n"
        f"  {LINE_2}\n"
        f"a:{EXTRA}\n\n"
        "q:hello\n"
        "a:world\n"
    )
    table = commands._parse_answer_txt(content)
    expected = [f"{LINE_1}\n{LINE_2}", EXTRA]
    assert table[commands._normalize_answer_q(NI_HAO)] == expected
    assert table[commands._normalize_answer_q(NIN_HAO)] == expected
    assert table[commands._normalize_answer_q("hello")] == ["world"]


def test_lookup_fixed_answers_exact_only(monkeypatch) -> None:
    table = {
        commands._normalize_answer_q("hello world"): ["exact"],
        commands._normalize_answer_q("hello"): ["exact-hello"],
    }
    monkeypatch.setattr(commands, "_ANSWER_CACHE", table, raising=False)
    monkeypatch.setattr(commands, "_reload_answer_cache_if_needed", lambda: None)

    assert commands._lookup_fixed_answers("  Hello   WORLD  ") == ["exact"]
    assert commands._lookup_fixed_answers("say hello to everyone") == []
    assert commands._lookup_fixed_answers("hello") == ["exact-hello"]


def test_lookup_keyword_answers_can_match_keyword_inside_long_message(monkeypatch) -> None:
    table = {
        commands._normalize_answer_q(NI_HAO): [NI_HAO],
        commands._normalize_answer_q(NIN_HAO): [NI_HAO],
    }
    monkeypatch.setattr(commands, "_KEYWORD_ANSWER_CACHE", table, raising=False)
    monkeypatch.setattr(commands, "_reload_keyword_answer_cache_if_needed", lambda: None)

    assert commands._lookup_keyword_answers(LONG_MSG_1) == [NI_HAO]
    assert commands._lookup_keyword_answers(LONG_MSG_2) == [NI_HAO]


def test_lookup_keyword_answers_prefers_longest_keyword_match(monkeypatch) -> None:
    table = {
        commands._normalize_answer_q(NI_HAO): ["short"],
        commands._normalize_answer_q(NI_HAO_YA): ["long"],
    }
    monkeypatch.setattr(commands, "_KEYWORD_ANSWER_CACHE", table, raising=False)
    monkeypatch.setattr(commands, "_reload_keyword_answer_cache_if_needed", lambda: None)

    assert commands._lookup_keyword_answers(LONG_MSG_3) == ["long"]


def test_lookup_keyword_answers_returns_empty_when_no_match(monkeypatch) -> None:
    table = {
        commands._normalize_answer_q("hello"): ["contains"],
    }
    monkeypatch.setattr(commands, "_KEYWORD_ANSWER_CACHE", table, raising=False)
    monkeypatch.setattr(commands, "_reload_keyword_answer_cache_if_needed", lambda: None)

    assert commands._lookup_keyword_answers("bye") == []


def test_media_or_emoji_only_message_placeholder_text() -> None:
    evt = {"message": "[\u56fe\u7247]"}
    assert commands._is_media_or_emoji_only_message(evt, "[\u56fe\u7247]")


def test_media_or_emoji_only_message_cq_image_only() -> None:
    evt = {"message": "[CQ:image,file=abc.jpg]"}
    assert commands._is_media_or_emoji_only_message(evt, "[CQ:image,file=abc.jpg]")


def test_media_or_emoji_only_message_segment_image_only() -> None:
    evt = {
        "message": [
            {"type": "image", "data": {"file": "abc.jpg"}},
        ]
    }
    assert commands._is_media_or_emoji_only_message(evt, "[\u56fe\u7247]")


def test_media_or_emoji_only_message_segment_with_text_is_not_filtered() -> None:
    evt = {
        "message": [
            {"type": "image", "data": {"file": "abc.jpg"}},
            {"type": "text", "data": {"text": "\u4f60\u597d"}},
        ]
    }
    assert not commands._is_media_or_emoji_only_message(evt, "\u4f60\u597d")


def test_keyword_text_message_rejects_video_with_filename_text() -> None:
    evt = {
        "message": [
            {"type": "video", "data": {"file": "abc.mp4"}},
            {"type": "text", "data": {"text": "abc.mp4"}},
        ]
    }
    assert not commands._is_keyword_text_message(evt, "abc.mp4")


def test_keyword_text_message_accepts_text_with_at_segment() -> None:
    evt = {
        "message": [
            {"type": "at", "data": {"qq": "42"}},
            {"type": "text", "data": {"text": "\u4f60\u597d"}},
        ]
    }
    assert commands._is_keyword_text_message(evt, "[CQ:at,qq=42] \u4f60\u597d")
