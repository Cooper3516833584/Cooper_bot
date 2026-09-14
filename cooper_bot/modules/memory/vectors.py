from __future__ import annotations

import math

import numpy as np

# 向量空间的指纹前缀：只有同一 fingerprint 下的向量才互相比较。
FINGERPRINT_PREFIX = "memory-embed"
# 融合排序里向量分所占权重（其余给词法分）。
VECTOR_WEIGHT = 0.5


def fingerprint_for(model: object) -> str:
    """把 embedding 模型名变成 memory_embeddings.fingerprint 的取值。"""
    value = str(model or "").strip() or "unknown"
    return f"{FINGERPRINT_PREFIX}:{value}"


def encode(vector) -> bytes:
    """落库格式：原始 float64 字节（不归一化），与 vector_items 保持一致。"""
    return np.asarray([float(x) for x in vector], dtype=np.float64).reshape(-1).tobytes()


def decode(blob) -> list[float]:
    return [float(x) for x in np.frombuffer(bytes(blob), dtype=np.float64)]


def from_blob(blob) -> np.ndarray:
    """落库 BLOB -> 原始 float64 数组；维度校验由调用方用 size 完成。"""
    return np.frombuffer(bytes(blob), dtype=np.float64)


def _unit(array) -> np.ndarray | None:
    value = np.asarray(array, dtype=np.float64).reshape(-1)
    if value.size == 0:
        return None
    norm = float(np.linalg.norm(value))
    if not math.isfinite(norm) or norm <= 0.0:
        return None
    return value / norm


def normalize(vector) -> list[float] | None:
    """L2 归一化；空向量或零范数返回 None。"""
    unit = _unit(vector)
    return None if unit is None else [float(x) for x in unit]


def cosine(left, right) -> float:
    """两个任意向量的余弦；空/零范数/维度不一致一律返回 -1.0。"""
    a, b = _unit(left), _unit(right)
    if a is None or b is None or a.shape != b.shape:
        return -1.0
    value = float(np.dot(a, b))
    return value if math.isfinite(value) else -1.0


def similarity_scores(query, raw_vectors: list[np.ndarray]) -> list[float]:
    """批量余弦：查询向量只归一化一次；维度不一致或零范数返回 -1.0。"""
    if not raw_vectors:
        return []
    unit_query = _unit(query)
    if unit_query is None:
        return [-1.0] * len(raw_vectors)
    scores = []
    for vector in raw_vectors:
        unit = _unit(vector)
        if unit is None or unit.shape != unit_query.shape:
            scores.append(-1.0)
            continue
        value = float(np.dot(unit, unit_query))
        scores.append(value if math.isfinite(value) else -1.0)
    return scores


def fusion_order(
    facts: list[dict],
    lexical: dict[str, float],
    vector: dict[str, float],
    *,
    limit: int,
    min_similarity: float,
    vector_weight: float = VECTOR_WEIGHT,
) -> list[dict]:
    """词法分 + 余弦分融合排序。

    - 词法分按本批最大值归一化；低于 min_similarity 的余弦不计入向量分；
    - 两项都为 0 的事实不召回（与纯词法路径"无重叠不召回"一致）；
    - 排序键 (-score, -updated_at, fact_id) 保证结果确定。
    """
    peak = max([float(value) for value in lexical.values()], default=0.0)
    weight = min(1.0, max(0.0, float(vector_weight)))
    threshold = float(min_similarity)
    ranked = []
    for row in facts:
        fact_id = str(row.get("fact_id") or "")
        lex = (float(lexical.get(fact_id, 0.0)) / peak) if peak > 0.0 else 0.0
        raw = float(vector.get(fact_id, -1.0))
        vec = raw if raw >= threshold else 0.0
        if lex <= 0.0 and vec <= 0.0:
            continue
        score = (1.0 - weight) * lex + weight * vec
        ranked.append((score, float(row.get("updated_at") or 0.0), fact_id, row))
    ranked.sort(key=lambda item: (-item[0], -item[1], item[2]))
    return [item[3] for item in ranked[: max(1, int(limit))]]
