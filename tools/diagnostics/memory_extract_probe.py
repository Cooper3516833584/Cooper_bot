# tools/diagnostics/memory_extract_probe.py
"""诊断脚本：复现 chat memory 自动抽取的 ModelGatewayHTTPError。

背景：生产库里 extract 作业从 2026-09-15 起全部以 ``ModelGatewayHTTPError`` 失败，
但 ``cooper_bot/modules/memory/jobs.py`` 只记录异常类名，状态码与响应体都没有留下，
因此无法判断是参数被拒、请求体过大还是鉴权问题。

脚本只读生产库取一个真实失败批次，按 ``service.py:_extract_facts`` 完全相同的方式
组装 payload，然后用真实 ModelGateway 发一次请求，打印完整错误消息与 usage；若失败，
再用同一 payload 去掉 ``response_format``（json_mode=False）做一次对照，用于区分
"网关不支持 json_mode / thinking" 与 "内容或体积被拒"。

只发 2 次请求就结束，不重试、不批量。会把该批群聊正文发给 api_key.txt 里配置的网关，
这正是 AI_MEMORY_AUTO_EXTRACT_ENABLED=true 的既有行为。

用法：
    python tools/diagnostics/memory_extract_probe.py
    python tools/diagnostics/memory_extract_probe.py --scope qq:10101:public:group:30303 --actor 20202 --target 3747
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from cooper_bot.core import config  # noqa: E402
from cooper_bot.modules.ai import model_gateway as gateway_module  # noqa: E402
from cooper_bot.modules.memory.llm import MemoryLLM  # noqa: E402
from cooper_bot.modules.memory.service import _MEMORY_EXTRACT_SYSTEM  # noqa: E402

EVENT_TEXT_CHARS = 200
# 直接引用生产 prompt，避免探针自己维护一份漂移的副本。
EXTRACT_SYSTEM = _MEMORY_EXTRACT_SYSTEM

USAGE_LOG: list[dict] = []


def install_usage_capture() -> None:
    """包一层唯一的 HTTP 出口，记录每次调用的 usage / 耗时 / 状态。"""
    original = gateway_module.http_post_json

    def wrapped(url, payload, api_key, timeout=60.0):
        began = time.perf_counter()
        record: dict = {"url": url, "bytes": len(json.dumps(payload, ensure_ascii=False)), "json_mode": "response_format" in payload}
        try:
            data = original(url, payload, api_key, timeout=timeout)
        except Exception as exc:
            record.update(ok=False, seconds=time.perf_counter() - began, error=f"{type(exc).__name__}: {exc}")
            USAGE_LOG.append(record)
            raise
        record.update(
            ok=True,
            seconds=time.perf_counter() - began,
            usage=(data or {}).get("usage"),
            model=(data or {}).get("model"),
            finish_reason=str((((data or {}).get("choices") or [{}])[0] or {}).get("finish_reason") or ""),
        )
        USAGE_LOG.append(record)
        return data

    gateway_module.http_post_json = wrapped


def load_events(db_path: Path, scope_id: str, actor: int, cursor: int, target: int) -> list[dict]:
    con = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        scope = con.execute("SELECT * FROM memory_scopes WHERE scope_id=?", (scope_id,)).fetchone()
        if scope is None:
            raise SystemExit(f"scope 不存在：{scope_id}")
        rows = con.execute(
            "SELECT * FROM memory_events WHERE scope_id=? AND conversation_id=? AND actor_user_id=? AND role='user' "
            "AND seq>? AND seq<=? AND source_kind IN ('direct_chat','passive_chat') "
            "AND state IN ('completed','observed','unconfirmed') ORDER BY seq",
            (scope_id, scope["active_conversation_id"], actor, cursor, target),
        ).fetchall()
    finally:
        con.close()
    events: list[dict] = []
    for item in rows:
        row = dict(item)
        try:
            flags = json.loads(row.get("flags_json") or "{}")
        except (TypeError, ValueError):
            flags = {}
        if flags.get("metadata_only") or flags.get("truncated") or flags.get("unsafe_context_redacted") or not row.get("own_text"):
            continue
        events.append(row)
    return events


def build_payload(events: list[dict]) -> dict:
    return {
        "events": [
            {
                "input_seq": row["seq"],
                "own_text": str(row["own_text"])[:EVENT_TEXT_CHARS],
                "source_kind": row["source_kind"],
                "created_at": row["created_at"],
            }
            for row in events
        ]
    }


def build_gateway():
    providers = gateway_module.load_providers(config.AI_API_KEY_PATH)
    chat_cfg = providers.get(gateway_module.DEEPSEEK_PROVIDER)
    print(f"provider: model={getattr(chat_cfg, 'model', '?')} ready={getattr(chat_cfg, 'ready', '?')} base={getattr(chat_cfg, 'base_url', '')}")
    return gateway_module.ModelGateway(None, api_key_path=config.AI_API_KEY_PATH, provider_source=lambda: providers)


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scope", default=config.MEMORY_PROBE_SCOPE)
    parser.add_argument("--actor", type=int, default=config.MEMORY_PROBE_ACTOR)
    parser.add_argument("--cursor", type=int, default=0)
    parser.add_argument("--target", type=int, default=3747)
    args = parser.parse_args()
    if not args.scope or args.actor <= 0:
        parser.error("set --scope and --actor, or configure MEMORY_PROBE_SCOPE/MEMORY_PROBE_ACTOR")

    events = load_events(Path(config.AI_MEMORY_DB_PATH), args.scope, args.actor, args.cursor, args.target)
    payload = build_payload(events)
    body = json.dumps(payload, ensure_ascii=False)
    print(f"events={len(events)} payload_chars={len(body)} chars≈{len(body.encode('utf-8'))} bytes")

    install_usage_capture()
    gateway = build_gateway()
    llm = MemoryLLM(gateway)

    print("\n=== call 1: json_mode=True（与生产完全一致） ===")
    first_error = ""
    try:
        value = await llm.json(EXTRACT_SYSTEM, payload)
        print("OK:", json.dumps(value, ensure_ascii=False)[:500])
    except Exception as exc:
        first_error = f"{type(exc).__name__}: {exc}"
        print("FAIL:", first_error)

    if first_error:
        print("\n=== call 2: 同 payload，json_mode=False（对照） ===")
        messages = [
            {"role": "system", "content": EXTRACT_SYSTEM},
            {"role": "user", "content": body},
        ]
        try:
            raw = await asyncio.to_thread(gateway.chat, messages, temperature=0.0, json_mode=False, thinking=False, timeout=30.0)
            print("OK:", str(raw)[:500])
        except Exception as exc:
            print("FAIL:", f"{type(exc).__name__}: {exc}")

    print("\n=== usage ===")
    for index, record in enumerate(USAGE_LOG, 1):
        print(index, json.dumps(record, ensure_ascii=False))

    total_prompt = sum(int((r.get("usage") or {}).get("prompt_tokens") or 0) for r in USAGE_LOG)
    total_completion = sum(int((r.get("usage") or {}).get("completion_tokens") or 0) for r in USAGE_LOG)
    print(f"calls={len(USAGE_LOG)} prompt_tokens={total_prompt} completion_tokens={total_completion}")
    print("\n=== 结论 ===")
    if not first_error:
        print("抽取调用本身成功：上游不再拒绝，可直接跑存量。")
    else:
        print(f"抽取调用仍失败：{first_error}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
