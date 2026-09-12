from __future__ import annotations

from typing import Any

import pytest

from cooper_bot.modules.ai.aisvc import AIService


class _DummyLog:
    def info(self, _msg: str) -> None:
        return

    def warning(self, _msg: str) -> None:
        return


def _new_service() -> AIService:
    svc = AIService(log=_DummyLog())
    svc.deepseek_base_url = "https://example.local/v1"
    svc.deepseek_api_key = "fake-deepseek-key"
    svc.system_prompt = "system-prompt"
    return svc


def test_deepseek_task_text_keeps_legacy_stateless_payload(monkeypatch) -> None:
    svc = _new_service()
    captured: list[dict[str, Any]] = []

    def _fake_post_json(_url: str, payload: dict, _api_key: str, timeout: float = 90.0) -> dict:
        assert timeout == 90.0
        captured.append(payload)
        return {"choices": [{"message": {"content": "task-reply"}}]}

    monkeypatch.setattr("cooper_bot.modules.ai.model_gateway.http_post_json", _fake_post_json)

    assert svc.deepseek_task_ready is True
    assert svc._deepseek_task_text_sync("生成日历文案") == "task-reply"
    assert captured[0]["model"] == svc.chat_model
    assert captured[0]["temperature"] == svc._CHAT_TEMPERATURE
    assert captured[0]["thinking"] == {"type": "enabled"}
    assert captured[0]["reasoning_effort"] == svc._REASONING_EFFORT_HIGH


@pytest.mark.asyncio
async def test_deepseek_task_text_async_wrapper_uses_task_path(monkeypatch) -> None:
    svc = _new_service()
    monkeypatch.setattr(svc, "_deepseek_task_text_sync", lambda text: f"task:{text}")

    assert await svc.deepseek_task_text("hello") == "task:hello"


def test_readiness_properties_track_gateway_provider_state() -> None:
    """AIService 的 readiness 必须与 gateway provider 状态一致（含切换配置后）。"""
    svc = AIService(log=_DummyLog())

    # 未加载任何凭据：provider 未就绪，三个 readiness 都为 False。
    assert svc.gateway.provider("deepseek").ready is False
    assert svc.gateway.provider("embedding").ready is False
    assert svc.deepseek_task_ready is False
    assert svc.notice_ready is False
    assert svc.semantic_ready is False

    svc.deepseek_base_url = "https://example.local/v1"
    svc.deepseek_api_key = "fake-key"
    svc.system_prompt = "system-prompt"

    assert svc.gateway.provider("deepseek").ready is True
    assert svc.deepseek_task_ready is True
    assert svc.notice_ready is True
    # 向量索引未加载，embedding 也未配置，语义检索仍不可用。
    assert svc.gateway.provider("embedding").ready is False
    assert svc.semantic_ready is False

    svc.embedding_base_url = "https://embed.local/v1"
    svc.embedding_api_key = "embed-key"

    assert svc.gateway.provider("embedding").ready is True

    svc.deepseek_api_key = ""
    svc.embedding_api_key = ""

    assert svc.gateway.provider("deepseek").ready is False
    assert svc.gateway.provider("embedding").ready is False
    assert svc.deepseek_task_ready is False
    assert svc.notice_ready is False
    assert svc.semantic_ready is False


def test_load_api_config_uses_fixed_line_positions(tmp_path) -> None:
    """凭据按行位读取；embedding 取第 3、4 行，不会被第 5、6 行的 vision 配置顶上来的。"""
    path = tmp_path / "api_key.txt"
    path.write_text(
        "\n".join(
            [
                "https://ds.example/v1",
                "ds-key",
                "https://embed.example/v1",
                "embed-key",
                "https://vision.example/v1",
                "vision-key",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    svc = AIService(log=_DummyLog())
    svc.api_key_path = path
    svc._load_api_config()

    assert svc.deepseek_base_url == "https://ds.example/v1"
    assert svc.deepseek_api_key == "ds-key"
    assert svc.embedding_base_url == "https://embed.example/v1"
    assert svc.embedding_api_key == "embed-key"


def test_load_api_config_keeps_embedding_empty_when_its_lines_blank(tmp_path) -> None:
    """第 3、4 行为空时 embedding 保持为空，不会把第 5、6 行的 vision 当成 embedding。

    与改动前一致：必填行未全部就绪时只告警、不写入任何凭据。
    """

    class _CollectingLog:
        def __init__(self) -> None:
            self.warnings: list[str] = []

        def info(self, _msg: str) -> None:
            return

        def warning(self, msg: str) -> None:
            self.warnings.append(str(msg))

    path = tmp_path / "api_key.txt"
    path.write_text(
        "\n".join(["https://ds.example/v1", "ds-key", "", "", "https://vision.example/v1", "vision-key"]) + "\n",
        encoding="utf-8",
    )

    log = _CollectingLog()
    svc = AIService(log=log)
    svc.api_key_path = path
    svc._load_api_config()

    assert svc.deepseek_base_url == ""
    assert svc.deepseek_api_key == ""
    assert svc.embedding_base_url == ""
    assert svc.embedding_api_key == ""
    assert any("api_key.txt" in msg for msg in log.warnings)


def test_load_api_config_warns_when_required_lines_missing(tmp_path) -> None:
    class _CollectingLog:
        def __init__(self) -> None:
            self.warnings: list[str] = []

        def info(self, _msg: str) -> None:
            return

        def warning(self, msg: str) -> None:
            self.warnings.append(str(msg))

    path = tmp_path / "api_key.txt"
    path.write_text("https://ds.example/v1\nds-key\n", encoding="utf-8")

    log = _CollectingLog()
    svc = AIService(log=log)
    svc.api_key_path = path
    svc._load_api_config()

    assert any("api_key.txt" in msg for msg in log.warnings)
    assert svc.deepseek_base_url == ""


# ============ 联网搜索 compose prompt ============


def test_web_search_compose_prompt_has_no_internal_marker(monkeypatch) -> None:
    """compose prompt 是业务文本，不得残留内部/展示层引用标记。"""
    svc = _new_service()
    captured: dict = {}

    def _fake_chat(messages, **_kwargs):
        captured["messages"] = messages
        return "最终回答"

    monkeypatch.setattr(svc.gateway, "chat", _fake_chat)

    out = svc._web_search_compose_final_sync("系统提示", "用户问题原文", "素材原文")

    assert out == "最终回答"
    system = captured["messages"][0]["content"]
    user = captured["messages"][-1]["content"]
    assert "cite" not in system.lower()
    assert "cite" not in user.lower()
    assert "联网搜索结果" in user
    assert "用户问题原文" in user
    assert "素材原文" in user


def test_web_search_compose_keeps_material_and_strips_search_marker(monkeypatch) -> None:
    svc = _new_service()
    captured: dict = {}

    def _fake_chat(messages, **_kwargs):
        captured["messages"] = messages
        return "[WEB_SEARCH] 泄漏的查询词\n真正的回答"

    monkeypatch.setattr(svc.gateway, "chat", _fake_chat)

    out = svc._web_search_compose_final_sync("系统提示", "用户问题", "素材")

    assert out == "真正的回答"
    assert "[WEB_SEARCH]" not in out
