# cooper_bot/modules/ai/search_bridge.py
"""Kimi CLI 搜索桥。

kimi CLI 的 WebSearch 工具只认 Moonshot 的搜索服务协议：

    POST <base_url>
    Authorization: Bearer <api_key>
    {"text_query": "<查询词>"}
    -> {"search_results": [{"title", "url", "snippet", "date", "site_name"}]}

本模块在 127.0.0.1 上提供一个只服务该协议的本地端点，把请求转成 DeepSeek 的联网搜索
（Anthropic messages 端点 + 服务端 web_search 工具），再把结果翻译回上面的结构。
抓取与结果映射统一由 ``model_gateway`` 实现，本模块只负责协议翻译与端点安全边界。
端点只绑回环地址，并要求 Bearer token 与 public home 配置里的
``[services.moonshot_search].api_key`` 完全一致。
"""
from __future__ import annotations

import hmac
import json
import threading
import tomllib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable, Optional

from cooper_bot.core import config
from cooper_bot.modules.ai import model_gateway
from cooper_bot.modules.ai.model_gateway import ModelGateway

# 检索模型、结果条数与截断长度统一由 model_gateway 决定（实测结论见该模块顶部说明）。
# 这里保留同名常量，供既有调用方与测试读取。
_MAX_RESULTS = model_gateway.MAX_SEARCH_RESULTS
_MAX_ANSWER_CHARS = model_gateway.MAX_ANSWER_CHARS
# 兼容既有调用方：端点归一化实现已统一到 model_gateway。
_anthropic_messages_url = model_gateway.anthropic_messages_url

_MAX_CONCURRENCY = 4
_MAX_BODY_BYTES = 64 * 1024
_ALLOWED_PATHS = ("/search", "/search/")


class KimiSearchBridge:
    """把 Moonshot 搜索协议翻译成 DeepSeek 联网搜索的本地端点。"""

    def __init__(
        self,
        log,
        *,
        enabled: Optional[bool] = None,
        port: Optional[int] = None,
        timeout_seconds: Optional[float] = None,
        public_home: Optional[Path] = None,
        api_key_path: Optional[Path] = None,
        deepseek_caller: Optional[Callable[[str], list]] = None,
    ) -> None:
        self.log = log
        self.enabled = bool(config.AI_KIMI_SEARCH_BRIDGE_ENABLED if enabled is None else enabled)
        self.port = int(config.AI_KIMI_SEARCH_BRIDGE_PORT if port is None else port)
        self.timeout_seconds = float(
            config.AI_KIMI_SEARCH_BRIDGE_TIMEOUT_SECONDS if timeout_seconds is None else timeout_seconds
        )
        self.public_home = Path(public_home if public_home is not None else config.AI_KIMI_PUBLIC_HOME)
        self.api_key_path = Path(api_key_path if api_key_path is not None else config.AI_API_KEY_PATH)
        # 联网抓取统一走 gateway：它按 api_key_path 解析 deepseek 凭据与检索模型。
        self.gateway = ModelGateway(log, api_key_path=self.api_key_path)
        self._deepseek_caller = deepseek_caller or self._call_deepseek_search
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._slots = threading.BoundedSemaphore(_MAX_CONCURRENCY)
        self._token_lock = threading.Lock()
        self._token_cache: tuple[float, str] = (-1.0, "")

    # ---------- 生命周期 ----------

    def start(self) -> bool:
        """启动端点；端口不可用等失败只记日志，不影响 bot 其他功能。"""
        if not self.enabled:
            self.log.info("Kimi 搜索桥：已通过配置关闭，WebSearch 桥接未启用")
            return False
        if self._server is not None:
            return True
        try:
            server = ThreadingHTTPServer(("127.0.0.1", self.port), _make_handler(self))
        except OSError as e:
            self.log.warning(f"Kimi 搜索桥：端口 {self.port} 不可用，WebSearch 桥接未启用: {e}")
            return False
        self._server = server
        self.port = int(server.server_address[1])
        self._thread = threading.Thread(target=server.serve_forever, name="kimi-search-bridge", daemon=True)
        self._thread.start()
        self.log.info(f"Kimi 搜索桥：已监听 http://127.0.0.1:{self.port}/search （后端 DeepSeek 联网搜索）")
        return True

    def stop(self) -> None:
        server, self._server = self._server, None
        if server is not None:
            try:
                server.shutdown()
            except Exception:
                pass
            try:
                server.server_close()
            except Exception:
                pass
        thread, self._thread = self._thread, None
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
        self.log.info("Kimi 搜索桥：已停止")

    # ---------- 凭据 ----------

    def _expected_token(self) -> str:
        """读取 public home 配置里的搜索端点令牌（按 mtime 缓存）。"""
        path = self.public_home / "config.toml"
        try:
            mtime = path.stat().st_mtime
        except OSError:
            return ""
        with self._token_lock:
            if self._token_cache[0] == mtime:
                return self._token_cache[1]
        token = ""
        try:
            with path.open("rb") as fh:
                raw = tomllib.load(fh)
            services = raw.get("services") if isinstance(raw, dict) else None
            search = services.get("moonshot_search") if isinstance(services, dict) else None
            value = search.get("api_key") if isinstance(search, dict) else None
            if isinstance(value, str):
                token = value.strip()
        except (OSError, tomllib.TOMLDecodeError):
            token = ""
        with self._token_lock:
            self._token_cache = (mtime, token)
        return token

    # ---------- DeepSeek 联网搜索 ----------

    def _call_deepseek_search(self, query: str) -> list:
        """把查询交给 gateway；检索模型、端点与请求体由 gateway 的 search provider 决定。"""
        return self.gateway.search_web(query, timeout=self.timeout_seconds)

    @staticmethod
    def map_search_payload(payload: dict, query: str) -> list:
        """兼容入口：结果映射实现已统一到 model_gateway。"""
        return model_gateway.map_search_payload(payload, query)

    # ---------- HTTP 处理 ----------

    def _handle_post(self, handler: BaseHTTPRequestHandler) -> None:
        if handler.path.split("?", 1)[0] not in _ALLOWED_PATHS:
            _send_json(handler, 404, {"error": "not_found"})
            return
        token = self._expected_token()
        provided = _bearer_token(handler.headers.get("Authorization") or "")
        if not token or not provided or not hmac.compare_digest(provided, token):
            _send_json(handler, 401, {"error": "unauthorized"})
            return
        try:
            length = int(handler.headers.get("Content-Length") or 0)
        except ValueError:
            length = -1
        if length <= 0 or length > _MAX_BODY_BYTES:
            _send_json(handler, 413 if length > _MAX_BODY_BYTES else 400, {"error": "invalid_body"})
            return
        try:
            payload = json.loads(handler.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            _send_json(handler, 400, {"error": "invalid_json"})
            return
        query = str(payload.get("text_query") or "").strip() if isinstance(payload, dict) else ""
        if not query:
            _send_json(handler, 400, {"error": "empty_query"})
            return
        if not self._slots.acquire(timeout=self.timeout_seconds):
            self.log.warning("Kimi 搜索桥：并发检索已满，本次请求被拒绝")
            _send_json(handler, 503, {"error": "busy"})
            return
        try:
            results = self._deepseek_caller(query)
        except TimeoutError:
            self.log.warning("Kimi 搜索桥：上游检索超时")
            _send_json(handler, 504, {"error": "upstream_timeout"})
            return
        except Exception as e:
            self.log.warning(f"Kimi 搜索桥：检索失败 {type(e).__name__}: {str(e)[:120]}")
            _send_json(handler, 502, {"error": "search_failed"})
            return
        finally:
            self._slots.release()
        _send_json(handler, 200, {"search_results": results[:_MAX_RESULTS]})


def _bearer_token(header: str) -> str:
    value = str(header or "").strip()
    if not value.lower().startswith("bearer "):
        return ""
    return value[7:].strip()


def _send_json(handler: BaseHTTPRequestHandler, status: int, payload: dict) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    try:
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Content-Length", str(len(body)))
        if status >= 400:
            # 出错时不再复用连接，避免未读完的请求体把 keep-alive 连接弄脏。
            handler.send_header("Connection", "close")
            handler.close_connection = True
        handler.end_headers()
        handler.wfile.write(body)
    except (BrokenPipeError, ConnectionResetError):
        return


def _make_handler(bridge: KimiSearchBridge):
    class _SearchBridgeHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "CooperSearchBridge/1.0"

        def do_POST(self) -> None:  # noqa: N802
            bridge._handle_post(self)

        def do_GET(self) -> None:  # noqa: N802
            _send_json(self, 405, {"error": "method_not_allowed"})

        def log_message(self, *args) -> None:
            return

    return _SearchBridgeHandler


def start_kimi_search_bridge(log) -> KimiSearchBridge:
    """启动搜索桥并返回实例（供 client.py 调用）。"""
    bridge = KimiSearchBridge(log)
    bridge.start()
    return bridge
