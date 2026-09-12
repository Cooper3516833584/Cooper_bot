from __future__ import annotations

import http.client
import json
from pathlib import Path

import pytest

from cooper_bot.modules.ai import search_bridge
from cooper_bot.modules.ai.search_bridge import KimiSearchBridge


class _Log:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def info(self, message: str) -> None:
        self.messages.append(("info", message))

    def warning(self, message: str) -> None:
        self.messages.append(("warning", message))

    def error(self, message: str) -> None:
        self.messages.append(("error", message))


TOKEN = "unit-test-token"


def _home(tmp_path: Path, *, token: str = TOKEN) -> Path:
    home = tmp_path / "home"
    home.mkdir(parents=True, exist_ok=True)
    (home / "config.toml").write_text(
        "[tools]\nenabled = [\"WebSearch\"]\n\n[services.moonshot_search]\n"
        f'base_url = "http://127.0.0.1:18783/search"\napi_key = "{token}"\n',
        encoding="utf-8",
    )
    return home


def _bridge(tmp_path: Path, caller, **kwargs) -> KimiSearchBridge:
    bridge = KimiSearchBridge(
        _Log(),
        port=kwargs.pop("port", 0),
        public_home=_home(tmp_path, token=kwargs.pop("token", TOKEN)),
        api_key_path=tmp_path / "api_key.txt",
        deepseek_caller=caller,
        **kwargs,
    )
    assert bridge.start() is True
    return bridge


def _post(bridge: KimiSearchBridge, body, *, token: str | None = TOKEN, path: str = "/search"):
    conn = http.client.HTTPConnection("127.0.0.1", bridge.port, timeout=5)
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    payload = body if isinstance(body, (bytes, str)) else json.dumps(body)
    conn.request("POST", path, body=payload, headers=headers)
    response = conn.getresponse()
    raw = response.read()
    conn.close()
    return response.status, json.loads(raw.decode("utf-8"))


@pytest.fixture(autouse=True)
def _stop_bridges():
    created: list[KimiSearchBridge] = []
    original_start = search_bridge.KimiSearchBridge.start

    def _start(self):
        result = original_start(self)
        created.append(self)
        return result

    search_bridge.KimiSearchBridge.start = _start
    try:
        yield
    finally:
        search_bridge.KimiSearchBridge.start = original_start
        for bridge in created:
            bridge.stop()


def test_bridge_requires_matching_bearer_token(tmp_path) -> None:
    bridge = _bridge(tmp_path, lambda _q: [{"title": "t", "url": "https://e.com", "snippet": "s"}])

    assert _post(bridge, {"text_query": "hello"}, token=None)[0] == 401
    assert _post(bridge, {"text_query": "hello"}, token="wrong")[0] == 401


def test_bridge_returns_mapped_results_for_valid_request(tmp_path) -> None:
    bridge = _bridge(
        tmp_path,
        lambda query: [{"title": f"T-{query}", "url": "https://e.com/a", "snippet": "S", "date": "2026-09-12"}],
    )

    status, body = _post(bridge, {"text_query": "天气"})

    assert status == 200
    assert body == {
        "search_results": [
            {"title": "T-天气", "url": "https://e.com/a", "snippet": "S", "date": "2026-09-12"}
        ]
    }


@pytest.mark.parametrize(
    ("body", "token", "path", "expected"),
    [
        ("{not json", TOKEN, "/search", 400),
        ({"text_query": "   "}, TOKEN, "/search", 400),
        ({"query": "wrong key"}, TOKEN, "/search", 400),
        ({"text_query": "x"}, TOKEN, "/other", 404),
        ("OVERSIZED", TOKEN, "/search", 413),
    ],
    ids=["invalid-json", "empty-query", "wrong-field", "wrong-path", "oversized-body"],
)
def test_bridge_rejects_bad_requests(tmp_path, body, token, path, expected) -> None:
    bridge = _bridge(tmp_path, lambda _q: [{"title": "t", "url": "https://e.com", "snippet": "s"}])
    if body == "OVERSIZED":
        body = b"x" * (search_bridge._MAX_BODY_BYTES + 1)

    assert _post(bridge, body, token=token, path=path)[0] == expected


def test_bridge_reports_upstream_failure(tmp_path) -> None:
    def _boom(_query: str):
        raise RuntimeError("upstream down")

    bridge = _bridge(tmp_path, _boom)

    assert _post(bridge, {"text_query": "x"})[0] == 502


def test_bridge_can_be_disabled(tmp_path) -> None:
    log = _Log()
    bridge = KimiSearchBridge(log, enabled=False, port=0, public_home=_home(tmp_path))

    assert bridge.start() is False
    assert any(level == "info" for level, _ in log.messages)


def test_map_search_payload_builds_summary_and_sources() -> None:
    payload = {
        "content": [
            {"type": "thinking", "thinking": "..."},
            {"type": "server_tool_use", "name": "web_search", "input": {"query": "天气"}},
            {
                "type": "web_search_tool_result",
                "content": [
                    {"type": "web_search_result", "title": "中央气象台", "url": "https://weather.com.cn/a", "page_age": "2026-09-12"},
                    {"type": "web_search_result", "title": "和风天气", "url": "https://qweather.com/b", "page_age": ""},
                    {"type": "web_search_result", "title": "无链接", "url": ""},
                ],
            },
            {"type": "text", "text": "上海今天晴，最高 30 度。"},
        ]
    }

    results = KimiSearchBridge.map_search_payload(payload, "上海天气")

    assert results[0]["title"].startswith("联网检索摘要")
    assert results[0]["snippet"] == "上海今天晴，最高 30 度。"
    assert results[0]["url"] == "https://weather.com.cn/a"
    assert [item["url"] for item in results[1:]] == ["https://qweather.com/b"]
    assert results[1]["site_name"] == "qweather.com"
    assert results[1]["date"] == ""


def test_map_search_payload_truncates_long_answer_and_allows_sources_only() -> None:
    long_answer = "字" * (search_bridge._MAX_ANSWER_CHARS + 500)
    payload = {
        "content": [
            {"type": "text", "text": long_answer},
            {
                "type": "web_search_tool_result",
                "content": [{"type": "web_search_result", "title": "T", "url": "https://e.com/1"}],
            },
        ]
    }

    results = KimiSearchBridge.map_search_payload(payload, "q")
    assert len(results[0]["snippet"]) == search_bridge._MAX_ANSWER_CHARS

    sources_only = {
        "content": [
            {
                "type": "web_search_tool_result",
                "content": [{"type": "web_search_result", "title": "T", "url": "https://e.com/2"}],
            }
        ]
    }
    results = KimiSearchBridge.map_search_payload(sources_only, "q")
    assert results == [
        {"title": "T", "url": "https://e.com/2", "snippet": "", "date": "", "site_name": "e.com"}
    ]


@pytest.mark.parametrize(
    "payload",
    [
        {"content": []},
        {"content": [{"type": "web_search_tool_result", "content": [{"type": "web_search_tool_result_error"}]}]},
        {"unexpected": True},
    ],
)
def test_map_search_payload_raises_without_usable_results(payload) -> None:
    with pytest.raises(RuntimeError):
        KimiSearchBridge.map_search_payload(payload, "q")


@pytest.mark.parametrize(
    ("base_url", "expected"),
    [
        ("https://api.deepseek.com/v1", "https://api.deepseek.com/anthropic/v1/messages"),
        ("https://api.deepseek.com/v1/", "https://api.deepseek.com/anthropic/v1/messages"),
        ("https://api.deepseek.com", "https://api.deepseek.com/anthropic/v1/messages"),
        ("https://proxy.example/deepseek", "https://proxy.example/deepseek/anthropic/v1/messages"),
    ],
)
def test_anthropic_messages_url_normalizes_base(base_url, expected) -> None:
    assert search_bridge._anthropic_messages_url(base_url) == expected


def test_expected_token_follows_config_file(tmp_path) -> None:
    bridge = KimiSearchBridge(_Log(), port=0, public_home=_home(tmp_path, token="first"))
    assert bridge._expected_token() == "first"

    config_path = bridge.public_home / "config.toml"
    config_path.write_text(
        "[services.moonshot_search]\nbase_url = \"http://127.0.0.1:1/search\"\napi_key = \"second\"\n",
        encoding="utf-8",
    )
    import os

    os.utime(config_path, (config_path.stat().st_atime, config_path.stat().st_mtime + 1))
    assert bridge._expected_token() == "second"

    config_path.write_text("not = [valid toml", encoding="utf-8")
    os.utime(config_path, (config_path.stat().st_atime, config_path.stat().st_mtime + 1))
    assert bridge._expected_token() == ""


# ============ 总超时预算（排队 + 上游共享一个 deadline）============


class _RecordingGateway:
    """替换真实 ModelGateway，记录 query 与收到的 timeout。"""

    def __init__(self, *, error: Optional[BaseException] = None) -> None:
        self.calls: list[tuple[str, Optional[float]]] = []
        self.error = error

    def search_web(self, query: str, *, timeout: Optional[float] = None) -> list:
        self.calls.append((query, timeout))
        if self.error is not None:
            raise self.error
        return [{"title": "t", "url": "https://e.example", "snippet": "s"}]


def _started_bridge(tmp_path: Path, **kwargs) -> KimiSearchBridge:
    """构造不带注入 caller 的桥，使请求走默认 gateway 路径。"""
    bridge = KimiSearchBridge(
        _Log(),
        port=0,
        public_home=_home(tmp_path),
        api_key_path=tmp_path / "api_key.txt",
        **kwargs,
    )
    assert bridge.start() is True
    return bridge


def test_bridge_busy_slot_returns_503_without_calling_upstream(tmp_path, monkeypatch) -> None:
    import threading
    import time

    monkeypatch.setattr(search_bridge, "_QUEUE_WAIT_SECONDS", 0.2)
    bridge = _started_bridge(tmp_path)
    bridge.timeout_seconds = 30.0
    bridge._slots = threading.BoundedSemaphore(1)
    gateway = _RecordingGateway()
    bridge.gateway = gateway
    assert bridge._slots.acquire(timeout=1.0) is True

    started = time.monotonic()
    try:
        status, body = _post(bridge, {"text_query": "天气"})
    finally:
        bridge._slots.release()

    assert status == 503
    assert body == {"error": "busy"}
    assert gateway.calls == []
    # 排队上限远小于总预算，不会等完整的上游 timeout。
    assert time.monotonic() - started < 5.0


def test_bridge_passes_remaining_budget_to_upstream(tmp_path) -> None:
    import threading
    import time

    bridge = _started_bridge(tmp_path)
    bridge.timeout_seconds = 5.0
    bridge._slots = threading.BoundedSemaphore(1)
    gateway = _RecordingGateway()
    bridge.gateway = gateway
    assert bridge._slots.acquire(timeout=1.0) is True

    def _release_later() -> None:
        time.sleep(0.6)
        bridge._slots.release()

    releaser = threading.Thread(target=_release_later, daemon=True)
    releaser.start()
    try:
        status, body = _post(bridge, {"text_query": "天气"})
    finally:
        releaser.join(timeout=5.0)

    assert status == 200
    assert gateway.calls and gateway.calls[0][0] == "天气"
    received = gateway.calls[0][1]
    assert received is not None
    # 排队已消耗部分预算，上游拿到的是剩余时间而不是完整 5 秒。
    assert 0 < received < 5.0


@pytest.mark.parametrize(
    ("error", "expected_status", "expected_error"),
    [
        (search_bridge.model_gateway.ModelGatewayTimeoutError("timed out"), 504, "upstream_timeout"),
        (TimeoutError("timed out"), 504, "upstream_timeout"),
        (search_bridge.model_gateway.ModelGatewayHTTPError("http 500: boom"), 502, "search_failed"),
        (RuntimeError("boom"), 502, "search_failed"),
    ],
)
def test_bridge_maps_upstream_failures_and_releases_slot(
    tmp_path,
    error: BaseException,
    expected_status: int,
    expected_error: str,
) -> None:
    bridge = _started_bridge(tmp_path)
    bridge.gateway = _RecordingGateway(error=error)

    status, body = _post(bridge, {"text_query": "天气"})

    assert status == expected_status
    assert body == {"error": expected_error}
    # 任何失败路径都必须把并发槽还回来。
    assert bridge._slots.acquire(timeout=0.2) is True
    bridge._slots.release()


def test_bridge_keeps_single_arg_injected_caller_working(tmp_path) -> None:
    received: list[str] = []

    def _caller(query: str) -> list:
        received.append(query)
        return [{"title": "t", "url": "https://e.example", "snippet": "s"}]

    bridge = _started_bridge(tmp_path, deepseek_caller=_caller)

    status, body = _post(bridge, {"text_query": "天气"})

    assert status == 200
    assert received == ["天气"]
    assert body["search_results"][0]["url"] == "https://e.example"
