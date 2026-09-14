from __future__ import annotations

import asyncio
import json
import re


_FACT_KEY_RE = re.compile(r"(?:preference|profile|goal|constraint|habit)\.[a-z0-9_]+(?:\.[a-z0-9_]+){0,2}\Z")


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
    required = {"schema_version", "topics", "current_goal", "decisions", "open_items", "uncertainties"}
    if set(value) != required or value.get("schema_version") != 1 or not isinstance(value.get("current_goal"), str):
        raise ValueError("invalid summary schema")
    for key in ("topics", "decisions", "open_items", "uncertainties"):
        if not isinstance(value.get(key), list) or not all(isinstance(item, str) for item in value[key]):
            raise ValueError("invalid summary schema")
    return value


def validate_facts(value: dict) -> list[dict]:
    if set(value) != {"schema_version", "facts"} or value.get("schema_version") != 1 or not isinstance(value.get("facts"), list):
        raise ValueError("invalid fact schema")
    results = []
    for raw in value["facts"][:5]:
        if not isinstance(raw, dict):
            continue
        fact_key = str(raw.get("fact_key") or "").strip()
        text = str(raw.get("text") or "").strip()
        quote = str(raw.get("evidence_quote") or "").strip()
        try:
            evidence_seq = int(raw.get("evidence_input_seq") or 0)
        except (TypeError, ValueError):
            continue
        if not _FACT_KEY_RE.fullmatch(fact_key) or not text or not quote or evidence_seq <= 0:
            continue
        results.append({"fact_key": fact_key, "text": text, "evidence_input_seq": evidence_seq, "evidence_quote": quote})
    return results
