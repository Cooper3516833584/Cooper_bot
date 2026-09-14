from __future__ import annotations

import asyncio
import json


class MemoryLLM:
    """Narrow adapter for private maintenance jobs; it never invokes Kimi/tools."""
    def __init__(self, gateway, timeout: float = 30.0):
        self.gateway, self.timeout = gateway, timeout

    async def json(self, system: str, payload: dict) -> dict:
        raw = await asyncio.to_thread(
            self.gateway.chat,
            [{"role": "system", "content": system}, {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
            temperature=0.0, json_mode=True, thinking=False, timeout=self.timeout,
        )
        result = json.loads(raw)
        if not isinstance(result, dict):
            raise ValueError("memory model returned non-object")
        return result


def validate_summary(value: dict) -> dict:
    allowed = {"schema_version", "topics", "current_goal", "decisions", "open_items", "uncertainties"}
    if set(value) - allowed or value.get("schema_version") != 1:
        raise ValueError("invalid summary schema")
    return value


def validate_facts(value: dict) -> list[dict]:
    if set(value) != {"schema_version", "facts"} or value.get("schema_version") != 1 or not isinstance(value.get("facts"), list):
        raise ValueError("invalid fact schema")
    return value["facts"][:5]
