from __future__ import annotations

from types import SimpleNamespace

import commands


def _fake_filesvc_roots():
    return [
        SimpleNamespace(name="public"),
        SimpleNamespace(name="friend"),
        SimpleNamespace(name="admin"),
    ]


def test_parse_find_args_keyword_only() -> None:
    filesvc = SimpleNamespace(roots=_fake_filesvc_roots())
    kw, in_dir = commands._parse_find_args("calculus", filesvc)
    assert kw == "calculus"
    assert in_dir is None


def test_parse_find_args_with_in_dir() -> None:
    filesvc = SimpleNamespace(roots=_fake_filesvc_roots())
    kw, in_dir = commands._parse_find_args("calculus notes public/math", filesvc)
    assert kw == "calculus notes"
    assert in_dir == "public/math"


def test_parse_find_args_without_valid_dir_suffix() -> None:
    filesvc = SimpleNamespace(roots=_fake_filesvc_roots())
    kw, in_dir = commands._parse_find_args("calculus final review", filesvc)
    assert kw == "calculus final review"
    assert in_dir is None


def test_parse_semantic_find_query() -> None:
    assert commands._parse_semantic_find_query('"考试安排"') == "考试安排"
    assert commands._parse_semantic_find_query("“实验报告模板”") == "实验报告模板"


def test_parse_semantic_find_query_rejects_invalid_tail() -> None:
    assert commands._parse_semantic_find_query('"考试安排" public') is None
    assert commands._parse_semantic_find_query("考试安排") is None


def test_extract_ai_chat_input_private_prefix() -> None:
    ctx = SimpleNamespace(scene="private_friend")
    out = commands._extract_ai_chat_input(ctx, evt={}, text="C你好", bot_nick="Cooepr_bot")
    assert out == "你好"


def test_extract_ai_chat_input_group_requires_mention() -> None:
    ctx = SimpleNamespace(scene="group")
    evt = {
        "self_id": "42",
        "message": [
            {"type": "at", "data": {"qq": "42"}},
            {"type": "text", "data": {"text": " 你好"}},
        ],
    }
    out = commands._extract_ai_chat_input(ctx, evt=evt, text="你好", bot_nick="Cooepr_bot")
    assert out == "你好"
