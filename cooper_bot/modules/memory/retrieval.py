from __future__ import annotations

import re


def tokens(text: str) -> set[str]:
    value = str(text or "").casefold()
    out = set(re.findall(r"[a-z0-9_]+", value))
    chinese = "".join(re.findall(r"[\u4e00-\u9fff]", value))
    out.update(chinese[i:i + 2] for i in range(max(0, len(chinese) - 1)))
    if chinese:
        out.add(chinese)
    return {x for x in out if x}


def search(rows: list[dict], query: str, limit: int = 6) -> list[dict]:
    q = tokens(query)
    if not q:
        return rows[:limit]
    ranked = []
    phrase = str(query or "").strip().casefold()
    for row in rows:
        text = str(row.get("text") or row.get("own_text") or "")
        words = tokens(text)
        overlap = len(q & words)
        if not overlap:
            continue
        score = overlap / len(q) + (2.0 if phrase and phrase in text.casefold() else 0.0)
        ranked.append((score, float(row.get("updated_at") or row.get("created_at") or 0), str(row.get("fact_id") or row.get("seq") or ""), row))
    ranked.sort(key=lambda x: (-x[0], -x[1], x[2]))
    return [x[3] for x in ranked[:limit]]
