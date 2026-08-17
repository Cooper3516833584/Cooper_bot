from __future__ import annotations

import numpy as np

from cooper_bot.modules.ai.aisvc import AIService


class _DummyLog:
    def info(self, _msg: str) -> None:
        return

    def warning(self, _msg: str) -> None:
        return


def _new_service() -> AIService:
    return AIService(log=_DummyLog())


def _meta_for(svc: AIService, rel: str, *, subject: str = "math") -> dict:
    filename = rel.split("/")[-1]
    return {
        "file_path": svc._to_store_rel(rel),
        "filename": filename,
        "subject": subject,
    }


def test_semantic_insert_and_replace_keep_mapping_consistent() -> None:
    svc = _new_service()

    with svc._lock:
        ok1 = svc._semantic_insert_or_replace_locked("math/a.pdf", _meta_for(svc, "math/a.pdf"), np.array([1.0, 0.0, 0.0]))
        ok2 = svc._semantic_insert_or_replace_locked("math/b.pdf", _meta_for(svc, "math/b.pdf"), np.array([0.0, 1.0, 0.0]))
        assert ok1 is True
        assert ok2 is True
        assert svc._semantic_active_count == 2
        assert svc._semantic_row_by_rel["math/a.pdf"] == 0
        assert svc._semantic_row_by_rel["math/b.pdf"] == 1

        replaced = svc._semantic_insert_or_replace_locked(
            "math/a.pdf",
            _meta_for(svc, "math/a.pdf", subject="updated"),
            np.array([1.0, 1.0, 0.0]),
        )
        assert replaced is True
        assert svc._semantic_active_count == 2
        assert svc._semantic_row_by_rel["math/a.pdf"] == 0
        assert svc._semantic_meta[0]["subject"] == "updated"
        assert np.isclose(np.linalg.norm(svc._semantic_norm_vectors[0]), 1.0)


def test_semantic_delete_compacts_rows_without_breaking_indexes() -> None:
    svc = _new_service()

    with svc._lock:
        svc._semantic_insert_or_replace_locked("math/a.pdf", _meta_for(svc, "math/a.pdf"), np.array([1.0, 0.0, 0.0]))
        svc._semantic_insert_or_replace_locked("math/b.pdf", _meta_for(svc, "math/b.pdf"), np.array([0.0, 1.0, 0.0]))
        svc._semantic_insert_or_replace_locked("math/c.pdf", _meta_for(svc, "math/c.pdf"), np.array([0.0, 0.0, 1.0]))

        svc._semantic_delete_locked("math/b.pdf")

        assert svc._semantic_active_count == 2
        assert "math/b.pdf" not in svc._semantic_row_by_rel
        assert set(svc._semantic_row_by_rel.keys()) == {"math/a.pdf", "math/c.pdf"}
        assert svc._semantic_row_by_rel["math/c.pdf"] == 1
        assert len(svc._semantic_meta) == 2
        assert len(svc._semantic_rel_by_row) == 2


def test_semantic_apply_changes_updates_upserts_and_deletes() -> None:
    svc = _new_service()
    metadata_by_rel = {
        "math/base.pdf": _meta_for(svc, "math/base.pdf"),
        "math/delete_me.pdf": _meta_for(svc, "math/delete_me.pdf"),
    }
    vector_by_rel = {
        "math/base.pdf": np.array([1.0, 0.0], dtype=np.float64),
        "math/delete_me.pdf": np.array([0.0, 1.0], dtype=np.float64),
    }
    svc._set_semantic_cache_from_maps(metadata_by_rel, vector_by_rel)

    svc._apply_semantic_cache_changes(
        metadata_upserts={
            "math/base.pdf": _meta_for(svc, "math/base.pdf", subject="replaced"),
            "math/new.pdf": _meta_for(svc, "math/new.pdf", subject="new"),
        },
        vector_upserts={
            "math/base.pdf": np.array([0.0, 2.0], dtype=np.float64),
            "math/new.pdf": np.array([2.0, 0.0], dtype=np.float64),
        },
        delete_rels={"math/delete_me.pdf"},
    )

    assert set(svc._semantic_entry_by_rel.keys()) == {"math/base.pdf", "math/new.pdf"}
    assert svc._semantic_active_count == 2
    assert len(svc._semantic_meta) == svc._semantic_active_count
    assert len(svc._semantic_rel_by_row) == svc._semantic_active_count
    assert len(svc._semantic_row_by_rel) == svc._semantic_active_count
    base_meta, _base_vec = svc._semantic_entry_by_rel["math/base.pdf"]
    assert base_meta["subject"] == "replaced"
