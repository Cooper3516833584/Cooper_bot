from __future__ import annotations

import json
from pathlib import Path

import pytest

from cooper_bot.modules.ai import model_gateway
from cooper_bot.modules.ai.model_gateway import ModelGateway


def _write_api_key(tmp_path: Path, lines: list[str]) -> Path:
    path = tmp_path / "api_key.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


# ============ provider 解析 ============


def test_load_providers_reads_six_lines_in_order(tmp_path: Path, monkeypatch) -> None:
    # 部署环境的 secrets.env 可能已提供 VISION_*，这里清空以验证文件行序回退。
    monkeypatch.setattr("cooper_bot.core.config.VISION_BASE_URL", "")
    monkeypatch.setattr("cooper_bot.core.config.VISION_API_KEY", "")
    # 与部署环境解耦：没有凭据的环境里 VISION_ENABLED 默认 False，会掩盖行位断言。
    monkeypatch.setattr("cooper_bot.core.config.VISION_ENABLED", True)
    path = _write_api_key(
        tmp_path,
        [
            "https://ds.example/v1",
            "ds-key",
            "https://embed.example/v1",
            "embed-key",
            "https://vision.example/v1",
            "vision-key",
        ],
    )

    providers = model_gateway.load_providers(path)

    assert providers["deepseek"].base_url == "https://ds.example/v1"
    assert providers["deepseek"].api_key == "ds-key"
    assert providers["embedding"].base_url == "https://embed.example/v1"
    assert providers["embedding"].api_key == "embed-key"
    assert providers["vision"].base_url == "https://vision.example/v1"
    assert providers["vision"].api_key == "vision-key"
    assert providers["deepseek"].ready is True
    assert providers["embedding"].ready is True
    assert providers["vision"].ready is True
    # Search API 复用 DeepSeek 凭据，不需要单独指定搜索模型。
    assert providers["search"].base_url == providers["deepseek"].base_url
    assert providers["search"].api_key == providers["deepseek"].api_key


def test_load_providers_with_four_lines_leaves_vision_unconfigured(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("cooper_bot.core.config.VISION_BASE_URL", "")
    monkeypatch.setattr("cooper_bot.core.config.VISION_API_KEY", "")
    # 与部署环境解耦：没有凭据的环境里 VISION_ENABLED 默认 False，会掩盖行位断言。
    monkeypatch.setattr("cooper_bot.core.config.VISION_ENABLED", True)
    path = _write_api_key(tmp_path, ["https://ds.example", "k", "https://embed.example", "e"])

    providers = model_gateway.load_providers(path)

    assert providers["deepseek"].ready is True
    assert providers["embedding"].ready is True
    assert providers["vision"].base_url == ""
    assert providers["vision"].ready is False


def test_load_providers_lets_env_override_vision_file_values(tmp_path: Path, monkeypatch) -> None:
    path = _write_api_key(
        tmp_path,
        ["https://ds.example", "k", "https://embed.example", "e", "https://file.example", "file-key"],
    )
    monkeypatch.setattr("cooper_bot.core.config.VISION_BASE_URL", "https://env.example/v1/")
    monkeypatch.setattr("cooper_bot.core.config.VISION_API_KEY", "env-key")

    providers = model_gateway.load_providers(path)

    assert providers["vision"].base_url == "https://env.example/v1"
    assert providers["vision"].api_key == "env-key"


def test_vision_enabled_flag_follows_config(tmp_path: Path, monkeypatch) -> None:
    path = _write_api_key(tmp_path, ["https://ds.example", "k", "https://e.example", "e", "https://v.example", "vk"])
    monkeypatch.setattr("cooper_bot.core.config.VISION_ENABLED", False)

    providers = model_gateway.load_providers(path)

    assert providers["vision"].enabled is False
    assert providers["vision"].ready is False


def test_model_names_come_from_config(tmp_path: Path, monkeypatch) -> None:
    path = _write_api_key(tmp_path, ["https://ds.example", "k", "https://e.example", "e"])
    monkeypatch.setattr("cooper_bot.core.config.AI_CHAT_MODEL", "chat-model-x")
    monkeypatch.setattr("cooper_bot.core.config.AI_EMBED_MODEL", "embed-model-z")

    providers = model_gateway.load_providers(path)

    assert providers["deepseek"].model == "chat-model-x"
    assert providers["search"].model == ""
    assert providers["search"].kind == model_gateway.KIND_SEARCH
    assert providers["search"].ready is True
    assert providers["embedding"].model == "embed-model-z"


def test_build_providers_falls_back_to_documented_defaults() -> None:
    providers = model_gateway.build_providers()

    assert providers["deepseek"].model == model_gateway.DEFAULT_CHAT_MODEL
    assert providers["search"].model == ""
    assert providers["embedding"].model == model_gateway.DEFAULT_EMBED_MODEL
    assert providers["vision"].model == model_gateway.DEFAULT_VISION_MODEL


def test_gateway_reload_keeps_path_credential_source(tmp_path: Path) -> None:
    path = _write_api_key(tmp_path, ["https://first.example", "k1", "https://e.example", "e"])
    gateway = ModelGateway(api_key_path=path)
    assert gateway.provider("deepseek").base_url == "https://first.example"

    path.write_text("\n".join(["https://second.example", "k2", "https://e.example", "e"]) + "\n", encoding="utf-8")
    gateway.reload()

    assert gateway.provider("deepseek").base_url == "https://second.example"


# ============ chat ============


def _gateway_with_deepseek() -> ModelGateway:
    return ModelGateway(
        provider_source=lambda: model_gateway.build_providers(
            deepseek_base_url="https://ds.example/v1",
            deepseek_api_key="k",
            chat_model="chat-model",
        )
    )


def test_chat_builds_expected_payload(monkeypatch) -> None:
    captured: dict = {}

    def _fake_post(url: str, payload: dict, api_key: str, timeout: float = 60.0) -> dict:
        captured.update(url=url, payload=payload, api_key=api_key, timeout=timeout)
        return {"choices": [{"message": {"content": "  回答  "}}]}

    monkeypatch.setattr(model_gateway, "http_post_json", _fake_post)
    gateway = _gateway_with_deepseek()

    text = gateway.chat(
        [{"role": "user", "content": "hi"}],
        temperature=0.0,
        json_mode=True,
        timeout=45.0,
    )

    assert text == "回答"
    assert captured["url"] == "https://ds.example/v1/chat/completions"
    assert captured["api_key"] == "k"
    assert captured["timeout"] == 45.0
    assert captured["payload"] == {
        "model": "chat-model",
        "messages": [{"role": "user", "content": "hi"}],
        "temperature": 0.0,
        "thinking": {"type": "disabled"},
        "response_format": {"type": "json_object"},
    }


def test_chat_enables_thinking_and_reasoning_effort(monkeypatch) -> None:
    captured: dict = {}

    def _fake_post(_url: str, payload: dict, _api_key: str, timeout: float = 60.0) -> dict:
        captured.update(payload)
        return {"choices": [{"message": {"content": "ok"}}]}

    monkeypatch.setattr(model_gateway, "http_post_json", _fake_post)

    _gateway_with_deepseek().chat([{"role": "user", "content": "hi"}], temperature=0.65, thinking=True)

    assert captured["thinking"] == {"type": "enabled"}
    assert captured["reasoning_effort"] == model_gateway.REASONING_EFFORT_HIGH
    assert "response_format" not in captured


def test_chat_returns_empty_string_when_content_missing(monkeypatch) -> None:
    monkeypatch.setattr(model_gateway, "http_post_json", lambda *_a, **_k: {"choices": []})

    assert _gateway_with_deepseek().chat([{"role": "user", "content": "hi"}]) == ""


def test_chat_rejects_non_text_provider(monkeypatch) -> None:
    gateway = _gateway_with_deepseek()

    with pytest.raises(RuntimeError):
        gateway.chat([{"role": "user", "content": "hi"}], provider="vision")


def test_unknown_provider_raises() -> None:
    with pytest.raises(RuntimeError):
        _gateway_with_deepseek().provider("nope")


# ============ search_web ============


def _search_payload() -> dict:
    return {
        "results": [
            {
                "title": "中央气象台",
                "url": "https://weather.example/a",
                "snippet": "上海晴。",
                "summary": "上海今天晴，最高 25 度。",
                "site_name": "中国天气网",
                "date": "2026-10-17T02:30:00Z",
                "rerank_score": 2.5,
                "authority": 3,
                "content": "",
            },
            {
                "title": "和风天气",
                "url": "https://qweather.example/b",
                "snippet": "上海天气预报",
                "summary": "",
                "site_name": None,
                "date": None,
                "rerank_score": 1.7,
                "authority": 2,
                "content": "",
            },
        ]
    }


def test_search_web_calls_deepseek_search_api_and_maps_results(monkeypatch) -> None:
    captured: dict = {}

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self) -> bytes:
            return json.dumps(_search_payload(), ensure_ascii=False).encode("utf-8")

    def _fake_urlopen(request, timeout=None):
        captured.update(
            url=request.full_url,
            method=request.get_method(),
            payload=json.loads(request.data.decode("utf-8")),
            headers=dict(request.headers),
            timeout=timeout,
        )
        return _Resp()

    monkeypatch.setattr(model_gateway.urllib.request, "urlopen", _fake_urlopen)

    results = _gateway_with_deepseek().search_web("上海天气", timeout=30.0)

    assert captured["url"] == "https://ds.example/v1/tools/search"
    assert captured["method"] == "POST"
    assert captured["headers"]["Authorization"] == "Bearer k"
    assert captured["headers"]["Content-type"] == "application/json"
    assert captured["timeout"] == 30.0
    assert captured["payload"] == {"query": "上海天气", "count": 5, "need_content": False}
    assert results[0]["title"] == "中央气象台"
    assert results[0]["snippet"] == "上海今天晴，最高 25 度。"
    assert results[0]["date"] == "2026-10-17T02:30:00Z"
    assert results[1]["url"] == "https://qweather.example/b"
    assert results[1]["snippet"] == "上海天气预报"
    assert results[1]["site_name"] == "qweather.example"
    assert "date" not in results[1]


def test_search_web_truncates_overlong_query(monkeypatch) -> None:
    captured: dict = {}

    def _fake_post(url: str, payload: dict, api_key: str, timeout: float = 60.0) -> dict:
        captured.update(payload)
        return {"results": []}

    monkeypatch.setattr(model_gateway, "http_post_json", _fake_post)
    assert _gateway_with_deepseek().search_web("中" * 200) == []
    assert len(captured["query"]) == 128


def test_search_web_empty_results_are_valid(monkeypatch) -> None:
    monkeypatch.setattr(model_gateway, "http_post_json", lambda *_a, **_k: {"results": []})
    assert _gateway_with_deepseek().search_web("没有搜索结果") == []


def test_search_web_rejects_empty_query() -> None:
    with pytest.raises(RuntimeError):
        _gateway_with_deepseek().search_web("   ")


def test_search_web_raises_when_provider_not_ready() -> None:
    gateway = ModelGateway(
        provider_source=lambda: model_gateway.build_providers()
    )

    with pytest.raises(RuntimeError):
        gateway.search_web("q")


def test_map_search_payload_uses_each_real_source() -> None:
    results = model_gateway.map_search_payload(_search_payload(), "上海天气")

    assert [item["url"] for item in results] == [
        "https://weather.example/a",
        "https://qweather.example/b",
    ]
    assert results[0]["site_name"] == "中国天气网"
    assert results[1]["site_name"] == "qweather.example"


def test_map_search_payload_deduplicates_filters_and_limits_results() -> None:
    payload = {"results": [
        {"title": "T", "url": "javascript:alert(1)", "summary": "bad"},
        {"title": "T", "url": "https://example.com/1", "summary": "a"},
        {"title": "Duplicate", "url": "https://example.com/1", "summary": "duplicate"},
        *({"title": "Another", "url": f"https://example.com/{i}", "summary": "b"} for i in range(2, 9)),
    ]}
    results = model_gateway.map_search_payload(payload, "q")
    assert len(results) == model_gateway.MAX_SEARCH_RESULTS
    assert results[0]["url"] == "https://example.com/1"
    assert len({item["url"] for item in results}) == len(results)


@pytest.mark.parametrize("payload", [{"content": []}, {"unexpected": True}, {"results": "x"}, {"results": [{"url": "javascript:alert(1)"}]}])
def test_map_search_payload_raises_for_invalid_results(payload: dict) -> None:
    with pytest.raises(RuntimeError):
        model_gateway.map_search_payload(payload, "q")


def test_search_results_to_text_keeps_title_url_and_snippet() -> None:
    text = model_gateway.search_results_to_text(
        [{"title": "T", "url": "https://e.example", "snippet": "S"}, {"title": "", "url": "", "snippet": ""}]
    )

    assert "标题：T" in text
    assert "来源：https://e.example" in text
    assert "S" in text
    assert text.count("\n\n") == 0


# ============ embed ============


def _embedding_gateway(*, ready: bool = True) -> ModelGateway:
    return ModelGateway(
        provider_source=lambda: model_gateway.build_providers(
            embedding_base_url="https://embed.example/v1" if ready else "",
            embedding_api_key="ek" if ready else "",
            embed_model="embed-model",
        )
    )


def test_embed_posts_model_and_input(monkeypatch) -> None:
    captured: dict = {}

    def _fake_post(url: str, payload: dict, api_key: str, timeout: float = 60.0) -> dict:
        captured.update(url=url, payload=payload, api_key=api_key, timeout=timeout)
        return {"data": [{"embedding": [0.25, 0.5]}]}

    monkeypatch.setattr(model_gateway, "http_post_json", _fake_post)

    assert _embedding_gateway().embed("文本") == [0.25, 0.5]
    assert captured["url"] == "https://embed.example/v1/embeddings"
    assert captured["payload"] == {"model": "embed-model", "input": "文本"}
    assert captured["api_key"] == "ek"
    assert captured["timeout"] == 90.0


def test_embed_returns_none_when_provider_not_ready(monkeypatch) -> None:
    def _unexpected(*_args, **_kwargs):
        raise AssertionError("request must not be attempted")

    monkeypatch.setattr(model_gateway, "http_post_json", _unexpected)

    assert _embedding_gateway(ready=False).embed("文本") is None


def test_embed_returns_none_and_warns_on_failure(monkeypatch) -> None:
    warnings: list[str] = []

    class _Log:
        def warning(self, message: str) -> None:
            warnings.append(message)

    def _boom(*_args, **_kwargs):
        raise RuntimeError("http 500: boom")

    monkeypatch.setattr(model_gateway, "http_post_json", _boom)
    gateway = ModelGateway(
        _Log(),
        provider_source=lambda: model_gateway.build_providers(
            embedding_base_url="https://embed.example", embedding_api_key="ek", embed_model="m"
        ),
    )

    assert gateway.embed("文本") is None
    assert warnings and "embedding" in warnings[0]


# ============ vision_describe ============


class _FakeVisionCompletions:
    def __init__(self, *, reject_max_tokens: bool = False, content: str = "描述") -> None:
        self.calls: list[dict] = []
        self.reject_max_tokens = reject_max_tokens
        self.content = content

    async def create(self, **kwargs):
        if self.reject_max_tokens and "max_tokens" in kwargs:
            raise TypeError("unexpected keyword argument 'max_tokens'")
        self.calls.append(kwargs)
        message = type("M", (), {"content": self.content})()
        choice = type("C", (), {"message": message})()
        return type("R", (), {"choices": [choice]})()


class _FakeVisionClient:
    def __init__(self, **kwargs) -> None:
        self.chat = type("Chat", (), {"completions": _FakeVisionCompletions(**kwargs)})()


def _vision_gateway() -> ModelGateway:
    return ModelGateway(
        provider_source=lambda: model_gateway.build_providers(
            vision_base_url="https://vision.example/v1",
            vision_api_key="vk",
            vision_model="qwen3.5-flash",
        )
    )


async def test_vision_describe_uses_injected_client_and_model() -> None:
    client = _FakeVisionClient()
    gateway = _vision_gateway()

    text = await gateway.vision_describe(
        "data:image/png;base64,AAA",
        system_prompt="系统提示",
        max_tokens=1600,
        client=client,
    )

    assert text == "描述"
    assert client.chat.completions.calls[0]["model"] == "qwen3.5-flash"
    assert client.chat.completions.calls[0]["max_tokens"] == 1600
    assert client.chat.completions.calls[0]["temperature"] == 0.0
    content = client.chat.completions.calls[0]["messages"][0]["content"]
    assert content[0] == {"type": "text", "text": "系统提示"}
    assert content[1]["image_url"]["url"] == "data:image/png;base64,AAA"


async def test_vision_describe_retries_without_max_tokens_on_type_error() -> None:
    client = _FakeVisionClient(reject_max_tokens=True)

    text = await _vision_gateway().vision_describe(
        "data:image/png;base64,AAA",
        system_prompt="系统提示",
        max_tokens=1600,
        client=client,
    )

    assert text == "描述"
    assert client.chat.completions.calls and "max_tokens" not in client.chat.completions.calls[0]


async def test_vision_describe_returns_empty_when_content_missing() -> None:
    client = _FakeVisionClient(content="")

    assert await _vision_gateway().vision_describe("u", system_prompt="p", client=client) == ""


# ============ 传输层 ============


def test_http_post_json_maps_http_error_to_runtime_error(monkeypatch) -> None:
    import urllib.error

    def _boom(*_args, **_kwargs):
        raise urllib.error.HTTPError("https://x", 401, "unauthorized", {}, None)

    monkeypatch.setattr(model_gateway.urllib.request, "urlopen", _boom)

    with pytest.raises(RuntimeError) as excinfo:
        model_gateway.http_post_json("https://x", {"a": 1}, "k")

    assert "http 401" in str(excinfo.value)


def test_http_post_json_rejects_non_json_body(monkeypatch) -> None:
    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self) -> bytes:
            return b"not json"

    monkeypatch.setattr(model_gateway.urllib.request, "urlopen", lambda *_a, **_k: _Resp())

    with pytest.raises(model_gateway.ModelGatewayError) as excinfo:
        model_gateway.http_post_json("https://x", {"a": 1}, "k")

    # 协议/解析类错误统一落到 ModelGatewayError，不是 timeout 也不是 HTTP 错误。
    assert not isinstance(excinfo.value, TimeoutError)
    assert not isinstance(excinfo.value, model_gateway.ModelGatewayHTTPError)


def test_post_json_with_headers_serializes_utf8_payload(monkeypatch) -> None:
    captured: dict = {}

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self) -> bytes:
            return json.dumps({"ok": True}).encode("utf-8")

    def _fake_urlopen(request, timeout=None):
        captured["body"] = request.data
        captured["headers"] = dict(request.headers)
        captured["timeout"] = timeout
        return _Resp()

    monkeypatch.setattr(model_gateway.urllib.request, "urlopen", _fake_urlopen)

    assert model_gateway.post_json_with_headers("https://x", {"q": "中文"}, {"x-api-key": "k"}, 10.0) == {"ok": True}
    assert json.loads(captured["body"].decode("utf-8")) == {"q": "中文"}
    assert captured["headers"]["X-api-key"] == "k"
    assert captured["timeout"] == 10.0


def test_deepseek_search_url_normalizes_v1_suffix() -> None:
    assert model_gateway.deepseek_search_url("https://api.deepseek.com/v1") == "https://api.deepseek.com/v1/tools/search"
    assert model_gateway.deepseek_search_url("https://api.deepseek.com/v1/") == "https://api.deepseek.com/v1/tools/search"
    assert model_gateway.deepseek_search_url("https://api.deepseek.com") == "https://api.deepseek.com/v1/tools/search"
    assert model_gateway.deepseek_search_url("https://proxy.example/deepseek") == "https://proxy.example/deepseek/v1/tools/search"


# ============ 传输层异常语义 ============


def _urlopen_raising(exc: BaseException):
    def _boom(*_args, **_kwargs):
        raise exc

    return _boom


def test_http_post_json_maps_timeout_to_typed_timeout_error(monkeypatch) -> None:
    monkeypatch.setattr(
        model_gateway.urllib.request,
        "urlopen",
        _urlopen_raising(TimeoutError("timed out")),
    )

    with pytest.raises(model_gateway.ModelGatewayTimeoutError) as excinfo:
        model_gateway.http_post_json("https://x", {"a": 1}, "k")

    # 上层可以继续用 except TimeoutError 分支处理。
    assert isinstance(excinfo.value, TimeoutError)
    assert isinstance(excinfo.value, model_gateway.ModelGatewayError)


def test_http_post_json_maps_url_error_timeout_reason(monkeypatch) -> None:
    import urllib.error

    monkeypatch.setattr(
        model_gateway.urllib.request,
        "urlopen",
        _urlopen_raising(urllib.error.URLError(TimeoutError("timed out"))),
    )

    with pytest.raises(model_gateway.ModelGatewayTimeoutError):
        model_gateway.http_post_json("https://x", {"a": 1}, "k")

    monkeypatch.setattr(
        model_gateway.urllib.request,
        "urlopen",
        _urlopen_raising(urllib.error.URLError("timed out")),
    )

    with pytest.raises(model_gateway.ModelGatewayTimeoutError):
        model_gateway.http_post_json("https://x", {"a": 1}, "k")


def test_http_post_json_maps_non_timeout_network_error(monkeypatch) -> None:
    import urllib.error

    monkeypatch.setattr(
        model_gateway.urllib.request,
        "urlopen",
        _urlopen_raising(urllib.error.URLError("connection refused")),
    )

    with pytest.raises(model_gateway.ModelGatewayError) as excinfo:
        model_gateway.http_post_json("https://x", {"a": 1}, "k")

    assert not isinstance(excinfo.value, TimeoutError)
    assert not isinstance(excinfo.value, model_gateway.ModelGatewayHTTPError)


def test_http_post_json_maps_http_error_to_typed_error(monkeypatch) -> None:
    import urllib.error

    monkeypatch.setattr(
        model_gateway.urllib.request,
        "urlopen",
        _urlopen_raising(urllib.error.HTTPError("https://x", 503, "unavailable", {}, None)),
    )

    with pytest.raises(model_gateway.ModelGatewayHTTPError) as excinfo:
        model_gateway.http_post_json("https://x", {"a": 1}, "k")

    assert "http 503" in str(excinfo.value)
    assert not isinstance(excinfo.value, TimeoutError)


def test_http_error_message_redacts_credentials(monkeypatch) -> None:
    import io
    import urllib.error

    secret = "sk-super-secret-value"
    body = json.dumps({"error": {"message": f"invalid key {secret}"}}).encode("utf-8")

    def _boom(*_args, **_kwargs):
        raise urllib.error.HTTPError("https://x", 401, "unauthorized", {}, io.BytesIO(body))

    monkeypatch.setattr(model_gateway.urllib.request, "urlopen", _boom)

    with pytest.raises(model_gateway.ModelGatewayHTTPError) as excinfo:
        model_gateway.http_post_json("https://x", {"a": 1}, secret)

    message = str(excinfo.value)
    assert secret not in message
    assert "<redacted>" in message


# ============ chat provider readiness ============


def test_chat_requires_ready_provider(monkeypatch) -> None:
    def _unexpected(*_args, **_kwargs):
        raise AssertionError("request must not be attempted")

    monkeypatch.setattr(model_gateway, "http_post_json", _unexpected)

    cases = [
        model_gateway.build_providers(),  # 全部为空
        model_gateway.build_providers(deepseek_base_url="https://ds.example/v1"),
        model_gateway.build_providers(deepseek_api_key="k"),
    ]

    for providers in cases:
        gateway = ModelGateway(
            provider_source=lambda providers=providers: providers
        )
        with pytest.raises(model_gateway.ModelGatewayError) as excinfo:
            gateway.chat([{"role": "user", "content": "hi"}])
        assert "not ready" in str(excinfo.value)


# ============ api_key.txt 固定行位 ============


def test_load_providers_keeps_fixed_positions_with_blank_lines(tmp_path: Path, monkeypatch) -> None:
    # 部署环境可能已提供 VISION_*，这里清空以验证 api_key.txt 的固定行位不被空行左移。
    monkeypatch.setattr("cooper_bot.core.config.VISION_BASE_URL", "")
    monkeypatch.setattr("cooper_bot.core.config.VISION_API_KEY", "")
    # 与部署环境解耦：没有凭据的环境里 VISION_ENABLED 默认 False，会掩盖行位断言。
    monkeypatch.setattr("cooper_bot.core.config.VISION_ENABLED", True)
    path = _write_api_key(
        tmp_path,
        ["https://ds.example/v1", "ds-key", "", "", "https://vision.example/v1", "vision-key"],
    )

    providers = model_gateway.load_providers(path)

    assert providers["deepseek"].base_url == "https://ds.example/v1"
    assert providers["deepseek"].api_key == "ds-key"
    assert providers["embedding"].base_url == ""
    assert providers["embedding"].api_key == ""
    assert providers["embedding"].ready is False
    assert providers["vision"].base_url == "https://vision.example/v1"
    assert providers["vision"].api_key == "vision-key"
    assert providers["vision"].ready is True


@pytest.mark.parametrize("count", [0, 1, 2, 3, 5, 6, 7])
def test_load_providers_tolerates_any_line_count(tmp_path: Path, monkeypatch, count: int) -> None:
    monkeypatch.setattr("cooper_bot.core.config.VISION_BASE_URL", "")
    monkeypatch.setattr("cooper_bot.core.config.VISION_API_KEY", "")
    # 与部署环境解耦：没有凭据的环境里 VISION_ENABLED 默认 False，会掩盖行位断言。
    monkeypatch.setattr("cooper_bot.core.config.VISION_ENABLED", True)
    path = _write_api_key(tmp_path, [f"line-{i}.example" for i in range(count)])

    providers = model_gateway.load_providers(path)

    assert set(providers) == {"deepseek", "search", "embedding", "vision"}
    if count >= 2:
        assert providers["deepseek"].base_url == "line-0.example"
        assert providers["deepseek"].api_key == "line-1.example"
    else:
        assert providers["deepseek"].ready is False
