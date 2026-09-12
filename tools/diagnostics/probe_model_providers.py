# tools/diagnostics/probe_model_providers.py
"""诊断脚本：核对 DeepSeek 模型 id 与联网检索能力。

按与 ``AIService._load_api_config()`` 相同的方式读取 ``config/private/api_key.txt``
的前两行（base url + key）作为凭据。脚本只输出模型 id、HTTP 状态码，以及是否观测到
服务端检索结果，**不打印任何 api key 或请求头**。

覆盖四组探测：
    1. GET  <base>/models                        —— 端点实际公布的模型 id
    2. POST <base>/chat/completions              —— 候选 id 是否可用
    3. POST <base>/anthropic/v1/messages         —— Anthropic 机制的服务端检索
    4. POST <base>/responses                     —— Responses 机制的服务端检索

第 3、4 组的差别很重要：``aisvc.py`` 用的是 Responses 机制，``search_bridge.py``
用的是 Anthropic 机制；两者的检索能力并不相同。

用法：
    python tools/diagnostics/probe_model_providers.py
"""
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from cooper_bot.core import config  # noqa: E402

CANDIDATE_CHAT_MODELS = ("deepseek-flash", "deepseek-v4-flash", "deepseek-v4-pro", "deepseek-v4.1-flash")
SEARCH_MODELS = ("deepseek-flash", "deepseek-v4-flash", "deepseek-v4-pro")
SEARCH_QUERY = "2026年9月12日 中国 重要新闻"
TIMEOUT_SECONDS = 60.0
CHAT_MAX_TOKENS = 256
SEARCH_MAX_TOKENS = 512
WEB_SEARCH_TOOL_TYPE = "web_search_20250305"
ANTHROPIC_VERSION = "2023-06-01"


def read_credentials() -> tuple[str, str]:
    try:
        lines = [
            x.strip()
            for x in config.AI_API_KEY_PATH.read_text(encoding="utf-8").splitlines()
            if x.strip()
        ]
    except OSError as e:
        print(f"[FATAL] cannot read api_key.txt: {type(e).__name__}")
        return "", ""
    if len(lines) < 2:
        print("[FATAL] api_key.txt needs at least 2 non-blank lines (base url + key)")
        return "", ""
    return lines[0].rstrip("/"), lines[1]


def join_url(base: str, endpoint: str) -> str:
    return f"{str(base or '').rstrip('/')}/{str(endpoint or '').lstrip('/')}"


def anthropic_messages_url(base: str) -> str:
    root = str(base or "").strip().rstrip("/")
    if root.endswith("/v1"):
        root = root[: -len("/v1")]
    return f"{root}/anthropic/v1/messages"


def redacted_host(base: str) -> str:
    try:
        parsed = urlparse(base)
        return f"{parsed.scheme}://{parsed.netloc}"
    except ValueError:
        return "<unparsable>"


def http_get(url: str, headers: dict) -> tuple[int | None, str]:
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def http_post(url: str, payload: dict, headers: dict) -> tuple[int | None, str]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def brief_error(body: str, limit: int = 240) -> str:
    """只提取错误消息本身，避免把响应体（可能含回显）整段打出来。"""
    try:
        obj = json.loads(body)
    except Exception:
        return body[:limit].replace("\n", " ")
    if isinstance(obj, dict):
        err = obj.get("error")
        if isinstance(err, dict):
            return str(err.get("message") or err.get("type") or err)[:limit]
        if err is not None:
            return str(err)[:limit]
        return str(obj.get("detail") or obj.get("message") or "")[:limit]
    return str(obj)[:limit]


def probe_model_list(base_url: str, api_key: str) -> None:
    print("\n=== [1/4] model list ===")
    if not base_url:
        print("skipped: no base url")
        return
    status, body = http_get(join_url(base_url, "models"), {"Authorization": f"Bearer {api_key}"})
    print(f"GET /models -> status={status}")
    if status != 200:
        print(f"  error: {brief_error(body)}")
        return
    try:
        ids = [
            str(item.get("id") or "")
            for item in (json.loads(body).get("data") or [])
            if isinstance(item, dict)
        ]
    except Exception as e:
        print(f"  unparsable body: {type(e).__name__}")
        return
    print(f"  {len(ids)} model id(s):")
    for model_id in sorted(x for x in ids if x):
        print(f"    - {model_id}")
    for wanted in CANDIDATE_CHAT_MODELS:
        present = "YES" if wanted in ids else "NO"
        print(f"  present[{wanted}] = {present}")


def probe_chat(base_url: str, api_key: str) -> None:
    print("\n=== [2/4] chat/completions per candidate model ===")
    if not base_url:
        print("skipped: no base url")
        return
    url = join_url(base_url, "chat/completions")
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    for model in CANDIDATE_CHAT_MODELS:
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": "reply with the single word ok"}],
            "temperature": 0.0,
            "max_tokens": CHAT_MAX_TOKENS,
        }
        status, body = http_post(url, payload, headers)
        if status == 200:
            try:
                choice = ((json.loads(body).get("choices") or [{}])[0] or {})
                content = str(choice.get("message", {}).get("content") or "")
                finish = str(choice.get("finish_reason") or "")
                note = f"content={content.strip()[:40]!r} finish_reason={finish}"
            except Exception:
                note = "body unparsable"
        else:
            note = brief_error(body, 160)
        print(f"  {model}: status={status} {note}")


def probe_anthropic_search(base_url: str, api_key: str) -> None:
    print("\n=== [3/4] anthropic /anthropic/v1/messages web_search ===")
    if not base_url:
        print("skipped: no base url")
        return
    url = anthropic_messages_url(base_url)
    print(f"endpoint: {redacted_host(base_url)}/anthropic/v1/messages")
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": ANTHROPIC_VERSION,
    }
    for model in SEARCH_MODELS:
        payload = {
            "model": model,
            "max_tokens": SEARCH_MAX_TOKENS,
            "system": "你是联网检索执行器。必须调用 web_search 工具检索，然后只输出简明的检索摘要。",
            "messages": [{"role": "user", "content": SEARCH_QUERY}],
            "tools": [{"type": WEB_SEARCH_TOOL_TYPE, "name": "web_search", "max_uses": 2}],
        }
        status, body = http_post(url, payload, headers)
        if status != 200:
            print(f"  {model}: status={status} {brief_error(body, 160)}")
            continue
        try:
            blocks = json.loads(body).get("content") or []
        except Exception:
            print(f"  {model}: status=200 body unparsable")
            continue
        types = [str(b.get("type") or "") for b in blocks if isinstance(b, dict)]
        hit = any(t in {"web_search_tool_result"} for t in types)
        results = 0
        for block in blocks:
            if isinstance(block, dict) and block.get("type") == "web_search_tool_result":
                content = block.get("content")
                if isinstance(content, list):
                    results += len([c for c in content if isinstance(c, dict)])
        print(
            f"  {model}: status=200 web_search_observed={'YES' if hit else 'NO'} "
            f"results={results} block_types={types}"
        )


def probe_responses_search(base_url: str, api_key: str) -> None:
    """探测 aisvc 目前使用的 Responses 机制是否真的执行服务端检索。"""
    print("\n=== [4/4] /responses web_search (aisvc legacy mechanism) ===")
    if not base_url:
        print("skipped: no base url")
        return
    url = join_url(base_url, "responses")
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    for model in SEARCH_MODELS:
        payload = {
            "model": model,
            "input": [
                {"role": "system", "content": "你是联网检索执行器。必须调用 web_search 工具检索。"},
                {"role": "user", "content": SEARCH_QUERY},
            ],
            "tools": [{"type": "web_search"}],
            "tool_choice": {"type": "web_search"},
        }
        status, body = http_post(url, payload, headers)
        if status != 200:
            print(f"  {model}: status={status} {brief_error(body, 160)}")
            continue
        try:
            output = json.loads(body).get("output") or []
        except Exception:
            print(f"  {model}: status=200 body unparsable")
            continue
        types = [str(item.get("type") or "") for item in output if isinstance(item, dict)]
        has_call = any(t == "function_call" for t in types)
        results = 0
        for item in output:
            if not isinstance(item, dict) or item.get("type") != "function_call":
                continue
            try:
                args = json.loads(str(item.get("arguments") or ""))
            except Exception:
                continue
            found = args.get("search_results") if isinstance(args, dict) else None
            if isinstance(found, list):
                results += len(found)
        print(
            f"  {model}: status=200 function_call={has_call} "
            f"search_results={results} output_types={types}"
        )


def main() -> int:
    base_url, api_key = read_credentials()
    if not base_url or not api_key:
        return 2
    print("=== probe_model_providers ===")
    print(f"base url host: {redacted_host(base_url)} (path hidden)")
    print("api key: <redacted>")
    print(f"chat model candidates: {', '.join(CANDIDATE_CHAT_MODELS)}")
    probe_model_list(base_url, api_key)
    probe_chat(base_url, api_key)
    probe_anthropic_search(base_url, api_key)
    probe_responses_search(base_url, api_key)
    print("\ndone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
