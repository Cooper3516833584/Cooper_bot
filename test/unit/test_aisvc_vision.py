from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import aisvc
import commands
from aisvc import AIService


class _DummyLog:
    def info(self, _msg: str) -> None:
        return

    def warning(self, _msg: str) -> None:
        return


def _new_service() -> AIService:
    svc = AIService(log=_DummyLog())
    svc.deepseek_base_url = "https://example.local/v1"
    svc.deepseek_api_key = "fake-chat-key"
    svc.system_prompt = "system-prompt"
    return svc


def test_extract_evt_images() -> None:
    evt = {
        "message": [
            {"type": "text", "data": {"text": "看图"}},
            {"type": "image", "data": {"file": "f1", "url": "https://a/1.png"}},
            {"type": "face", "data": {"id": "1"}},
            {"type": "image", "data": {"url": "https://a/2.png"}},
            {"type": "image", "data": {}},
            {"type": "at", "data": {"qq": "123"}},
        ]
    }
    out = commands._extract_evt_images(evt)
    assert out == [{"file": "f1", "url": "https://a/1.png"}, {"file": "", "url": "https://a/2.png"}]
    assert commands._extract_evt_images({"message": []}) == []
    assert commands._extract_evt_images({}) == []


def test_recent_images_cache_ttl_and_isolation(controlled_time) -> None:
    svc = _new_service()
    svc.vision_cache_ttl = 600

    svc.record_recent_images("group:20001", [{"file": "f1", "url": "https://a/1.png"}])
    controlled_time.advance(120)
    svc.record_recent_images("group:20001", [{"file": "f2", "url": "https://a/2.png"}])
    svc.record_recent_images("private:10001", [{"file": "f3", "url": "https://a/3.png"}])

    g = svc._load_recent_images("group:20001")
    assert [x["url"] for x in g] == ["https://a/1.png", "https://a/2.png"]
    p = svc._load_recent_images("private:10001")
    assert [x["url"] for x in p] == ["https://a/3.png"]

    controlled_time.advance(601)
    assert svc._load_recent_images("group:20001") == []
    assert svc.record_recent_images("group:20001", []) is None


def test_recent_images_cache_trims_to_cap() -> None:
    svc = _new_service()
    svc.vision_cache_ttl = 600
    cap = svc._VISION_CACHE_MAX_ENTRIES_PER_SESSION
    for i in range(cap + 10):
        svc.record_recent_images("group:20001", [{"file": f"f{i}", "url": f"https://a/{i}.png"}])
    assert len(svc._load_recent_images("group:20001")) == cap


def test_ensure_agy_vision_allow_rule_idempotent(monkeypatch, tmp_project_root) -> None:
    svc = _new_service()
    svc.vision_enabled = True
    svc.vision_cache_dir = tmp_project_root / "vision_cache"
    settings_path = tmp_project_root / "settings.json"
    settings_path.write_text(json.dumps({"enableTelemetry": False, "model": "x"}, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(svc, "_agy_settings_path", lambda: settings_path)

    assert svc._ensure_agy_vision_allow_rule() is True
    obj1 = json.loads(settings_path.read_text(encoding="utf-8"))
    assert obj1["enableTelemetry"] is False
    assert obj1["model"] == "x"
    assert isinstance(obj1["permissions"]["allow"], list)
    assert obj1["permissions"]["allow"][0].startswith("read_file(")
    assert str(svc.vision_cache_dir.resolve()) in obj1["permissions"]["allow"][0]

    assert svc._ensure_agy_vision_allow_rule() is True
    obj2 = json.loads(settings_path.read_text(encoding="utf-8"))
    assert obj2 == obj1


def test_ensure_agy_vision_allow_rule_disabled_or_missing(monkeypatch, tmp_project_root) -> None:
    svc = _new_service()
    svc.vision_enabled = False
    monkeypatch.setattr(svc, "_agy_settings_path", lambda: tmp_project_root / "settings.json")
    assert svc._ensure_agy_vision_allow_rule() is False


class _FakeURL:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def __enter__(self) -> "_FakeURL":
        return self

    def __exit__(self, *args: Any) -> bool:
        return False

    def read(self, n: int = -1) -> bytes:
        return self._data


def test_download_image_sync(monkeypatch, tmp_project_root) -> None:
    svc = _new_service()
    target = tmp_project_root / "a.jpg"
    fake_png = b"\x89PNG" + b"x" * 100

    monkeypatch.setattr(aisvc.urllib.request, "urlopen", lambda _req, timeout=15.0: _FakeURL(fake_png))
    assert svc._download_image_sync("https://x/a.jpg", target) is True
    assert target.read_bytes()[:4] == b"\x89PNG"

    assert svc._download_image_sync("https://x/b.jpg", tmp_project_root / "b.jpg", max_bytes=10) is False
    assert svc._download_image_sync("", tmp_project_root / "c.jpg") is False


def test_describe_images_via_gemini(monkeypatch) -> None:
    svc = _new_service()
    calls: list[tuple] = []

    def fake_run(prompt, model, restricted=False, timeout_seconds=None):
        calls.append((prompt, model, restricted, timeout_seconds))
        return "图里有一只猫"

    monkeypatch.setattr(svc, "_run_gemini_cli_sync", fake_run)

    out = svc._describe_images_via_gemini_sync(["C:/x/a.jpg", "C:/x/b.jpg"], "这是什么")
    assert out == "图里有一只猫"
    prompt, model, restricted, _t = calls[0]
    assert "C:/x/a.jpg" in prompt
    assert "C:/x/b.jpg" in prompt
    assert "这是什么" in prompt
    assert restricted is True
    assert "read_file" in prompt

    assert svc._describe_images_via_gemini_sync([], "问题") == ""


def test_build_gemini_prompt_with_image_paths() -> None:
    svc = _new_service()

    p = svc._build_gemini_cli_prompt("sys", [], "问题", ["C:/x/a.jpg"])
    assert "C:/x/a.jpg" in p

    rp = svc._build_restricted_gemini_cli_prompt("sys", [], "问题", ["C:/x/a.jpg"])
    assert "C:/x/a.jpg" in rp
    assert "read_file ONLY for the user-provided image files" in rp

    rp2 = svc._build_restricted_gemini_cli_prompt("sys", [], "问题")
    assert "no read_file" in rp2
    assert "read_file ONLY" not in rp2


def test_gemini_chat_with_context_passes_image_paths(monkeypatch, controlled_time) -> None:
    svc = _new_service()
    captured: dict[str, Any] = {}

    def fake_run(prompt, model, restricted=False, timeout_seconds=None):
        captured["prompt"] = prompt
        captured["restricted"] = restricted
        return "看图回答"

    monkeypatch.setattr(svc, "_run_gemini_cli_sync", fake_run)

    out = svc._gemini_chat_with_context_sync("private:10001", "看图", "gemini", True, ["C:/x/a.jpg"])
    assert out == "看图回答"
    assert "C:/x/a.jpg" in captured["prompt"]
    assert captured["restricted"] is True


def test_chat_with_context_vision_injects_description(monkeypatch, controlled_time) -> None:
    svc = _new_service()
    svc.vision_enabled = True
    payloads: list[dict[str, Any]] = []

    def fake_post_json(_url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        _ = timeout
        payloads.append(payload)
        return {"choices": [{"message": {"content": "reply"}}]}

    monkeypatch.setattr(svc, "_post_json", fake_post_json)
    monkeypatch.setattr(svc, "_describe_images_via_gemini_sync", lambda _paths, _q: "图里有一只猫")

    out = svc._chat_with_context_sync("private:10001", "这是什么", ["C:/x/a.jpg"])
    assert out == "reply"

    user_msg = payloads[0]["messages"][-1]["content"]
    assert "【附带图片描述】" in user_msg
    assert "图里有一只猫" in user_msg
    assert "这是什么" in user_msg

    history = svc._load_active_chat_history("private:10001")
    assert history[0] == {"role": "user", "content": "这是什么"}


def test_chat_with_context_vision_disabled_keeps_plain_content(monkeypatch, controlled_time) -> None:
    svc = _new_service()
    svc.vision_enabled = False
    payloads: list[dict[str, Any]] = []

    def fake_post_json(_url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        _ = timeout
        payloads.append(payload)
        return {"choices": [{"message": {"content": "reply"}}]}

    monkeypatch.setattr(svc, "_post_json", fake_post_json)

    out = svc._chat_with_context_sync("private:10001", "这是什么", ["C:/x/a.jpg"])
    assert out == "reply"
    assert payloads[0]["messages"][-1]["content"] == "这是什么"


def test_chat_with_context_vision_describe_failure_degrades(monkeypatch, controlled_time) -> None:
    svc = _new_service()
    svc.vision_enabled = True
    payloads: list[dict[str, Any]] = []

    def fake_post_json(_url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        _ = timeout
        payloads.append(payload)
        return {"choices": [{"message": {"content": "reply"}}]}

    def boom(_paths, _q):
        raise RuntimeError("antigravity busy")

    monkeypatch.setattr(svc, "_post_json", fake_post_json)
    monkeypatch.setattr(svc, "_describe_images_via_gemini_sync", boom)

    out = svc._chat_with_context_sync("private:10001", "这是什么", ["C:/x/a.jpg"])
    assert out == "reply"
    assert payloads[0]["messages"][-1]["content"] == "这是什么"
