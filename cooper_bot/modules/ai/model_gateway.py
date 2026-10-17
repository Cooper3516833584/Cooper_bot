# cooper_bot/modules/ai/model_gateway.py
"""统一的模型调用入口。

全项目所有模型请求都从这里发出：provider 的凭据、模型名、端点与请求体只在本模块
解析和拼装一次，业务侧只调用 ``chat`` / ``search_web`` / ``embed`` / ``vision_describe``
四个原语，不再自己拼 payload、不再自己判凭据是否就绪。

provider 一览：

    deepseek   文本任务（邮件分类、群通知判定/省流、日历文案、资料摘要、资料纠偏）
    search     DeepSeek Search API（直接返回结构化网页检索结果）
    embedding  向量化（/find 语义检索与资料索引）
    vision     图片描述（qwen，与 DeepSeek 无关，集群聊图片解析）

凭据取自 ``config/private/api_key.txt`` 的固定行位（空行必须保留位置，代表该行留空）：

    第 1 行 DeepSeek base url    第 2 行 DeepSeek api key
    第 3 行 Embedding base url   第 4 行 Embedding api key
    第 5 行 Vision base url      第 6 行 Vision api key

Vision 优先使用环境变量 / secrets.env；缺行按空字符串补齐，不抛异常。
"""
from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

from cooper_bot.core import config as core_config

try:
    from openai import AsyncOpenAI  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    AsyncOpenAI = None


DEEPSEEK_PROVIDER = "deepseek"
SEARCH_PROVIDER = "search"
EMBEDDING_PROVIDER = "embedding"
VISION_PROVIDER = "vision"

KIND_OPENAI_COMPAT = "openai_compat"
KIND_SEARCH = "search_api"
KIND_VISION = "openai_compat_vision"

# 端点实测（2026-09-12，/models）只公布 deepseek-flash 与 deepseek-v4-pro。
DEFAULT_CHAT_MODEL = "deepseek-flash"
DEFAULT_EMBED_MODEL = "BAAI/bge-m3"
DEFAULT_VISION_MODEL = "qwen3.5-flash"

REASONING_EFFORT_HIGH = "high"

_MAX_SEARCH_RESULTS = 5
_MAX_SEARCH_QUERY_CHARS = 128
_MAX_SNIPPET_CHARS = 4000

# Kimi Search Bridge 与内部摘要共用的搜索结果上限。
MAX_SEARCH_RESULTS = _MAX_SEARCH_RESULTS


@dataclass(frozen=True)
class ProviderConfig:
    """一个模型 provider 的完整调用配置。"""

    name: str
    kind: str
    base_url: str = ""
    api_key: str = ""
    model: str = ""
    enabled: bool = True

    @property
    def ready(self) -> bool:
        return bool(self.enabled and self.base_url and self.api_key and (self.kind == KIND_SEARCH or self.model))


def read_api_key_lines(api_key_path: Optional[Path] = None, log=None) -> List[str]:
    """按行位置读取 api_key.txt；保留空行，避免后面的 provider 配置整体左移。

    行序（与既有 6 行格式一致，不引入注释语法，每行都当作取值）：
        1 = deepseek base url，2 = deepseek api key，
        3 = embedding base url，4 = embedding api key，
        5 = vision base url，6 = vision api key。
    """
    path = Path(api_key_path) if api_key_path is not None else Path(core_config.AI_API_KEY_PATH)
    try:
        return [x.strip() for x in path.read_text(encoding="utf-8").splitlines()]
    except Exception as e:
        if log is not None and hasattr(log, "warning"):
            log.warning(f"AI 配置：读取 api_key.txt 失败: {e}")
        return []


def build_providers(
    *,
    deepseek_base_url: str = "",
    deepseek_api_key: str = "",
    embedding_base_url: str = "",
    embedding_api_key: str = "",
    vision_base_url: str = "",
    vision_api_key: str = "",
    vision_enabled: bool = True,
    chat_model: str = "",
    embed_model: str = "",
    vision_model: str = "",
) -> Dict[str, ProviderConfig]:
    """把模型名与凭据组装成 provider 表（唯一组装处）。"""
    base = str(deepseek_base_url or "").rstrip("/")
    key = str(deepseek_api_key or "")
    return {
        DEEPSEEK_PROVIDER: ProviderConfig(
            name=DEEPSEEK_PROVIDER,
            kind=KIND_OPENAI_COMPAT,
            base_url=base,
            api_key=key,
            model=str(chat_model or DEFAULT_CHAT_MODEL),
        ),
        SEARCH_PROVIDER: ProviderConfig(
            name=SEARCH_PROVIDER,
            kind=KIND_SEARCH,
            base_url=base,
            api_key=key,
        ),
        EMBEDDING_PROVIDER: ProviderConfig(
            name=EMBEDDING_PROVIDER,
            kind=KIND_OPENAI_COMPAT,
            base_url=str(embedding_base_url or "").rstrip("/"),
            api_key=str(embedding_api_key or ""),
            model=str(embed_model or DEFAULT_EMBED_MODEL),
        ),
        VISION_PROVIDER: ProviderConfig(
            name=VISION_PROVIDER,
            kind=KIND_VISION,
            base_url=str(vision_base_url or "").rstrip("/"),
            api_key=str(vision_api_key or ""),
            model=str(vision_model or DEFAULT_VISION_MODEL),
            enabled=bool(vision_enabled),
        ),
    }


def load_providers(api_key_path: Optional[Path] = None) -> Dict[str, ProviderConfig]:
    """按 api_key.txt 的行序解析全部 provider。

    行序（保留空行，缺行按空字符串补齐；不引入注释语法，每行都当作取值）：
        1 = deepseek base url，2 = deepseek api key，
        3 = embedding base url，4 = embedding api key，
        5 = vision base url，6 = vision api key。
    """
    # 补足到 6 行，避免位置型配置因缺行或空行而越界/左移。
    lines = read_api_key_lines(api_key_path) + [""] * 6
    return build_providers(
        deepseek_base_url=lines[0],
        deepseek_api_key=lines[1],
        embedding_base_url=lines[2],
        embedding_api_key=lines[3],
        vision_base_url=str(core_config.VISION_BASE_URL or lines[4]),
        vision_api_key=str(core_config.VISION_API_KEY or lines[5]),
        vision_enabled=bool(core_config.VISION_ENABLED),
        chat_model=str(core_config.AI_CHAT_MODEL or ""),
        embed_model=str(core_config.AI_EMBED_MODEL or ""),
        vision_model=str(core_config.VISION_MODEL or ""),
    )


def join_url(base: str, endpoint: str) -> str:
    return f"{str(base or '').rstrip('/')}/{str(endpoint or '').lstrip('/')}"


def deepseek_search_url(base_url: str) -> str:
    """从已有 DeepSeek base URL 构造 /v1/tools/search 端点。"""
    root = str(base_url or "").strip().rstrip("/")
    return f"{root}/tools/search" if root.endswith("/v1") else f"{root}/v1/tools/search"


class ModelGatewayError(RuntimeError):
    """模型网关调用失败的基类。"""


class ModelGatewayTimeoutError(ModelGatewayError, TimeoutError):
    """上游超时；同时是 TimeoutError，便于调用方按超时语义分支。"""


class ModelGatewayHTTPError(ModelGatewayError):
    """上游返回非 2xx 状态码。"""


def _is_timeout_reason(reason: object) -> bool:
    # socket.timeout 自 Python 3.10 起就是 TimeoutError 的别名，无需再单独判断。
    if isinstance(reason, TimeoutError):
        return True
    return "timed out" in str(reason or "").lower()


def _redact_headers(text: str, headers: dict) -> str:
    """抹掉文本里出现的请求头凭据，避免上游回显把 key 带进日志。"""
    out = str(text or "")
    for value in (headers or {}).values():
        token = str(value or "")
        candidates = [token]
        # Authorization 这类头把凭据放在 scheme 之后，裸凭据也要单独抹一次。
        parts = token.split()
        if len(parts) > 1:
            candidates.append(parts[-1])
        for candidate in candidates:
            if len(candidate) >= 8 and candidate in out:
                out = out.replace(candidate, "<redacted>")
    return out


def post_json_with_headers(url: str, payload: dict, headers: dict, timeout: float = 60.0) -> dict:
    """唯一的 HTTP 出口。

    失败语义分三类，便于上层按需分支：
        ModelGatewayTimeoutError  上游超时（可被 except TimeoutError 捕获）
        ModelGatewayHTTPError     上游返回非 2xx
        ModelGatewayError         其他传输层错误
    """
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url=url, data=body, method="POST")
    for name, value in headers.items():
        req.add_header(name, value)

    try:
        with urllib.request.urlopen(req, timeout=float(timeout)) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8", errors="replace")
        except Exception:
            detail = str(e)
        raise ModelGatewayHTTPError(f"http {e.code}: {_redact_headers(detail, headers)[:300]}") from e
    except TimeoutError as e:
        raise ModelGatewayTimeoutError(f"timeout: {e}") from e
    except urllib.error.URLError as e:
        if _is_timeout_reason(getattr(e, "reason", None)):
            raise ModelGatewayTimeoutError(f"timeout: {e.reason}") from e
        raise ModelGatewayError(str(e)) from e
    except Exception as e:
        raise ModelGatewayError(str(e)) from e

    txt = raw.decode("utf-8", errors="replace").strip()
    if not txt:
        raise ModelGatewayError("empty response")
    try:
        obj = json.loads(txt)
    except Exception as e:
        raise ModelGatewayError(f"json decode failed: {e}")
    if not isinstance(obj, dict):
        raise ModelGatewayError("invalid response type")
    return obj


def http_post_json(url: str, payload: dict, api_key: str, timeout: float = 60.0) -> dict:
    """OpenAI 兼容端点的 POST（Bearer 认证）。"""
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return post_json_with_headers(url, payload, headers, timeout)


def extract_chat_text(resp: dict) -> str:
    try:
        return str(
            (((resp or {}).get("choices") or [{}])[0] or {})
            .get("message", {})
            .get("content")
            or ""
        ).strip()
    except Exception:
        return ""


def _host_of(url: str) -> str:
    try:
        return urlparse(url).netloc
    except ValueError:
        return ""


def map_search_payload(payload: dict, query: str) -> List[dict]:
    """将 DeepSeek Search API 的 results 转为 Kimi 与内部问答通用的搜索结果。"""
    items = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise RuntimeError("unexpected deepseek search payload")

    results: list[dict] = []
    seen_urls: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        try:
            parsed = urlparse(url)
        except ValueError:
            continue
        if parsed.scheme not in ("http", "https") or not parsed.hostname or url in seen_urls:
            continue
        seen_urls.add(url)
        mapped = {
            "title": str(item.get("title") or "").strip(),
            "url": url,
            "snippet": str(item.get("summary") or item.get("snippet") or "").strip(),
            "date": str(item.get("date") or "").strip(),
            "site_name": str(item.get("site_name") or "").strip() or parsed.hostname,
        }
        out = _trim_result(mapped)
        if out:
            results.append(out)
        if len(results) >= _MAX_SEARCH_RESULTS:
            break

    if items and not results:
        raise RuntimeError("deepseek search returned no usable results")
    return results


def _trim_result(item: dict) -> dict:
    out = {
        "title": str(item.get("title") or "").strip()[:200],
        "url": str(item.get("url") or "").strip(),
        "snippet": str(item.get("snippet") or "").strip()[:_MAX_SNIPPET_CHARS],
    }
    date = str(item.get("date") or "").strip()
    if date:
        out["date"] = date[:64]
    site_name = str(item.get("site_name") or "").strip()
    if site_name:
        out["site_name"] = site_name[:120]
    return out if (out["title"] or out["url"] or out["snippet"]) else {}


def search_results_to_text(results: List[dict]) -> str:
    """把检索结果格式化成给模型整合用的素材文本。"""
    parts: list[str] = []
    for item in results or []:
        if not isinstance(item, dict):
            continue
        block: list[str] = []
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip()
        snippet = str(item.get("snippet") or "").strip()
        if title:
            block.append(f"标题：{title}")
        if url:
            block.append(f"来源：{url}")
        if snippet:
            block.append(snippet)
        if block:
            parts.append("\n".join(block))
    return "\n\n".join(parts).strip()


class ModelGateway:
    """统一模型入口。同步原语供 ``asyncio.to_thread`` 调用，vision 为原生异步。"""

    def __init__(
        self,
        log=None,
        *,
        api_key_path: Optional[Path] = None,
        provider_source: Optional[Callable[[], Dict[str, ProviderConfig]]] = None,
    ) -> None:
        self.log = log
        self.api_key_path = Path(api_key_path) if api_key_path is not None else Path(core_config.AI_API_KEY_PATH)
        self._lock = threading.RLock()
        self._default_providers = load_providers(self.api_key_path)
        self._provider_source = provider_source
        self._vision_clients: Dict[tuple, Any] = {}

    # ---------- provider 解析 ----------

    def reload(self) -> Dict[str, ProviderConfig]:
        """按 api_key.txt 重新解析 provider 表。"""
        with self._lock:
            self._default_providers = load_providers(self.api_key_path)
            return dict(self._default_providers)

    def providers(self) -> Dict[str, ProviderConfig]:
        if self._provider_source is not None:
            return dict(self._provider_source())
        with self._lock:
            return dict(self._default_providers)

    def provider(self, name: str) -> ProviderConfig:
        found = self.providers().get(name)
        if found is None:
            raise RuntimeError(f"unknown model provider: {name}")
        return found

    # ---------- 调用原语 ----------

    def chat(
        self,
        messages: List[dict],
        *,
        provider: str = DEEPSEEK_PROVIDER,
        temperature: float = 0.65,
        json_mode: bool = False,
        thinking: bool = False,
        reasoning_effort: Optional[str] = None,
        timeout: float = 60.0,
    ) -> str:
        """OpenAI 兼容的 chat/completions。返回助手文本（可能为空串）。"""
        cfg = self.provider(provider)
        if cfg.kind != KIND_OPENAI_COMPAT:
            raise ModelGatewayError(f"provider {provider} is not text-capable")
        if not cfg.ready:
            raise ModelGatewayError(f"provider {provider} not ready")
        payload: dict = {
            "model": str(cfg.model),
            "messages": list(messages),
            "temperature": float(temperature),
            "thinking": {"type": "enabled" if thinking else "disabled"},
        }
        if thinking:
            payload["reasoning_effort"] = str(reasoning_effort or REASONING_EFFORT_HIGH)
        if json_mode:
            payload["response_format"] = {"type": "json_object"}
        data = http_post_json(
            join_url(cfg.base_url, "chat/completions"),
            payload,
            cfg.api_key,
            timeout=float(timeout),
        )
        return extract_chat_text(data)

    def search_web(
        self,
        query: str,
        *,
        provider: str = SEARCH_PROVIDER,
        timeout: float = 60.0,
    ) -> List[dict]:
        """直接调用 DeepSeek Search API，返回可供 Kimi 和内部任务使用的搜索结果。"""
        cfg = self.provider(provider)
        if cfg.kind != KIND_SEARCH:
            raise ModelGatewayError(f"provider {provider} is not search-capable")
        q = str(query or "").strip()
        if not q:
            raise ModelGatewayError("empty web search query")
        if not cfg.ready:
            raise ModelGatewayError("search provider not ready")
        payload = {
            "query": q[:_MAX_SEARCH_QUERY_CHARS],
            "count": _MAX_SEARCH_RESULTS,
            "need_content": False,
        }
        data = http_post_json(
            deepseek_search_url(cfg.base_url),
            payload,
            cfg.api_key,
            timeout=float(timeout),
        )
        return map_search_payload(data, q)

    def embed(
        self,
        text: str,
        *,
        provider: str = EMBEDDING_PROVIDER,
        timeout: float = 90.0,
    ) -> Optional[List[float]]:
        """文本向量化；provider 未就绪或请求失败时返回 None。"""
        cfg = self.provider(provider)
        if not cfg.ready:
            return None
        payload = {"model": str(cfg.model), "input": str(text or "")}
        try:
            data = http_post_json(
                join_url(cfg.base_url, "embeddings"),
                payload,
                cfg.api_key,
                timeout=float(timeout),
            )
            arr = (((data or {}).get("data") or [{}])[0] or {}).get("embedding")
            if isinstance(arr, list) and arr:
                return [float(x) for x in arr]
        except Exception as e:
            self._warning(f"AI 向量：embedding 请求失败: {e}")
        return None

    async def vision_describe(
        self,
        data_url: str,
        *,
        system_prompt: str,
        provider: str = VISION_PROVIDER,
        max_tokens: Optional[int] = None,
        timeout: Optional[float] = None,
        client: Any = None,
    ) -> str:
        """图片描述；``client`` 非空时优先使用（测试注入用）。"""
        cfg = self.provider(provider)
        if client is None:
            if AsyncOpenAI is None:
                raise RuntimeError("openai sdk is not installed")
            if not cfg.ready:
                raise RuntimeError("vision provider not ready")
            client = self._vision_client(cfg, timeout)
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": str(system_prompt or "")},
                    {"type": "image_url", "image_url": {"url": str(data_url or "")}},
                ],
            }
        ]
        kwargs: dict = {
            "model": str(cfg.model),
            "messages": messages,
            "temperature": 0.0,
        }
        if max_tokens is not None:
            kwargs["max_tokens"] = int(max_tokens)
        try:
            completion = await client.chat.completions.create(**kwargs)
        except TypeError:
            # 部分兼容端点不接受 max_tokens，去掉后重试一次。
            kwargs.pop("max_tokens", None)
            completion = await client.chat.completions.create(**kwargs)
        try:
            return str(completion.choices[0].message.content or "").strip()
        except Exception:
            return ""

    def _vision_client(self, cfg: ProviderConfig, timeout: Optional[float]):
        cache_key = (str(cfg.base_url), str(cfg.api_key), str(cfg.model), timeout)
        with self._lock:
            cached = self._vision_clients.get(cache_key)
            if cached is not None:
                return cached
        client = AsyncOpenAI(
            api_key=cfg.api_key,
            base_url=cfg.base_url,
            timeout=float(timeout) if timeout else None,
            max_retries=1,
        )
        with self._lock:
            self._vision_clients = {cache_key: client}
        return client

    # ---------- 日志 ----------

    def _warning(self, message: str) -> None:
        log = self.log
        if log is not None and hasattr(log, "warning"):
            try:
                log.warning(message)
                return
            except Exception:
                pass

    def describe_provider(self, name: str) -> str:
        """给日志用的脱敏描述（不含 key）。"""
        cfg = self.provider(name)
        host = _host_of(cfg.base_url) or "<unset>"
        return f"{name}: model={cfg.model or '<unset>'} host={host} ready={cfg.ready}"


__all__ = [
    "DEEPSEEK_PROVIDER",
    "SEARCH_PROVIDER",
    "EMBEDDING_PROVIDER",
    "VISION_PROVIDER",
    "KIND_SEARCH",
    "KIND_OPENAI_COMPAT",
    "KIND_VISION",
    "DEFAULT_CHAT_MODEL",
    "DEFAULT_EMBED_MODEL",
    "DEFAULT_VISION_MODEL",
    "ProviderConfig",
    "ModelGateway",
    "build_providers",
    "load_providers",
    "read_api_key_lines",
    "join_url",
    "deepseek_search_url",
    "http_post_json",
    "post_json_with_headers",
    "extract_chat_text",
    "map_search_payload",
    "search_results_to_text",
]
