from __future__ import annotations

import copy
import json


def build_memory_context(snapshot, facts: list[dict], query: str, budget: int) -> dict | None:
    selected = facts[:]
    context = {"schema_version": 1, "data_only": True, "facts": [{"id": f["fact_id"], "text": f["text"], "source_kind": f["source_kind"], "updated_at": f["updated_at"]} for f in selected], "recalled_history": []}
    # Keep data JSON-safe and let the Kimi runner retain its hard command-line limit.
    while selected and len(str(context)) > budget:
        selected.pop()
        context["facts"] = [{"id": f["fact_id"], "text": f["text"], "source_kind": f["source_kind"], "updated_at": f["updated_at"]} for f in selected]
    return context if context["facts"] else None


def fit_prompt_budget(payload: dict, budget: int) -> dict:
    """Trim optional memory/history data without truncating the current input."""
    result = copy.deepcopy(payload)
    limit = max(1, int(budget))

    def _size() -> int:
        return len(json.dumps(result, ensure_ascii=False, separators=(",", ":")))

    memory = result.get("memory_context")
    while _size() > limit and isinstance(memory, dict) and memory.get("recalled_history"):
        memory["recalled_history"].pop()
    while _size() > limit and isinstance(memory, dict) and memory.get("facts"):
        memory["facts"].pop()
    if isinstance(memory, dict) and not memory.get("facts") and not memory.get("recalled_history"):
        result.pop("memory_context", None)
    while _size() > limit and result.get("conversation_history"):
        result["conversation_history"].pop(0)
    if _size() > limit:
        raise ValueError("current request exceeds the configured prompt budget")
    return result


def materialize_memory_events(rows: tuple[dict, ...] | list[dict], scene: str) -> list[dict]:
    history: list[dict] = []
    for row in rows:
        role = str(row.get("role") or "")
        if role not in {"user", "assistant"}:
            continue
        parts = [str(row.get("own_text") or "").strip()]
        quoted = str(row.get("quoted_text") or "").strip()
        visual = str(row.get("visual_text") or "").strip()
        if quoted:
            parts.append(f"[引用内容]\n{quoted}")
        if visual:
            parts.append(f"[视觉描述]\n{visual}")
        content = "\n".join(part for part in parts if part)
        if not content:
            continue
        if role == "user" and scene == "group":
            content = f"[发言人QQ:{int(row.get('actor_user_id') or 0)}]\n{content}"
        history.append({"role": role, "content": content})
    return history
