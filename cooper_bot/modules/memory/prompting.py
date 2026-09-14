from __future__ import annotations

import copy


def build_memory_context(snapshot, facts: list[dict], query: str, budget: int) -> dict | None:
    selected = facts[:]
    context = {"schema_version": 1, "data_only": True, "facts": [{"id": f["fact_id"], "text": f["text"], "source_kind": f["source_kind"], "updated_at": f["updated_at"]} for f in selected], "recalled_history": []}
    # Keep data JSON-safe and let the Kimi runner retain its hard command-line limit.
    while selected and len(str(context)) > budget:
        selected.pop()
        context["facts"] = [{"id": f["fact_id"], "text": f["text"], "source_kind": f["source_kind"], "updated_at": f["updated_at"]} for f in selected]
    return context if context["facts"] else None


def fit_prompt_budget(payload: dict, budget: int) -> dict:
    """Return a copy with only old history removed; never truncates current input."""
    result = copy.deepcopy(payload)
    while len(str(result)) > budget and result.get("conversation_history"):
        result["conversation_history"].pop(0)
    return result
