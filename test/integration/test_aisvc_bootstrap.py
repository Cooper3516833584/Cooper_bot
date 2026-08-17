from __future__ import annotations

from pathlib import Path

import numpy as np

from cooper_bot.modules.ai.aisvc import AIService


class _DummyLog:
    def info(self, _msg: str) -> None:
        return

    def warning(self, _msg: str) -> None:
        return


def _new_service(monkeypatch) -> AIService:
    svc = AIService(log=_DummyLog())
    svc.embedding_base_url = "https://embed.local/v1"
    svc.embedding_api_key = "fake-embed-key"
    monkeypatch.setattr(svc, "_load_api_config", lambda: None)
    return svc


def _seed_store_entry(svc: AIService, rel: str, *, summary: str = "seed summary") -> None:
    rel_norm = svc._normalize_rel(rel)
    subject = svc._subject_from_rel(rel_norm)
    filename = Path(rel_norm).name
    svc._persist_incremental_store_changes(
        index_upserts={
            rel_norm: {
                "file_path": svc._to_store_rel(rel_norm),
                "subject": subject,
                "filename": filename,
                "file_type": "pdf",
                "keywords": [subject],
                "summary": summary,
            }
        },
        index_deletes=set(),
        metadata_upserts={
            rel_norm: {
                "file_path": svc._to_store_rel(rel_norm),
                "filename": filename,
                "subject": subject,
            }
        },
        metadata_deletes=set(),
        vector_upserts={rel_norm: np.asarray([0.1, 0.2, 0.3], dtype=np.float64)},
        vector_deletes=set(),
    )


def _install_pipeline_stubs(monkeypatch, svc: AIService) -> None:
    def _fake_pipeline(rel: str, hint=None, build_vector: bool = True) -> dict:
        _ = (hint, build_vector)
        rel_norm = svc._normalize_rel(rel)
        subject = svc._subject_from_rel(rel_norm)
        filename = Path(rel_norm).name
        return {
            "index_item": {
                "file_path": svc._to_store_rel(rel_norm),
                "subject": subject,
                "filename": filename,
                "file_type": "pdf",
                "keywords": [subject],
                "summary": f"indexed:{filename}",
            },
            "metadata_item": {
                "file_path": svc._to_store_rel(rel_norm),
                "filename": filename,
                "subject": subject,
            },
            "embedding_text": f"embed:{rel_norm}",
        }

    monkeypatch.setattr(svc, "_run_new_file_pipeline", _fake_pipeline)
    monkeypatch.setattr(
        svc,
        "_build_vector_for_embedding_text",
        lambda _text: np.asarray([1.0, 0.0, 0.0], dtype=np.float64),
    )


def test_quick_bootstrap_loads_existing_incremental_store(monkeypatch) -> None:
    svc = _new_service(monkeypatch)
    _seed_store_entry(svc, "math/bootstrap_exists.pdf")

    svc._bootstrap_quick_sync_sync()

    assert "math/bootstrap_exists.pdf" in svc._semantic_entry_by_rel
    assert svc._semantic_active_count == 1
    assert svc.semantic_ready is True


def test_bootstrap_post_startup_sync_runs_incremental_flow(monkeypatch, test_config: dict) -> None:
    svc = _new_service(monkeypatch)
    _install_pipeline_stubs(monkeypatch, svc)
    monkeypatch.setattr(svc, "_auto_organize_materials_on_boot", lambda _index_list: ({}, {}, set()))

    seen_calls: list[dict] = []
    original_persist = svc._persist_incremental_store_changes

    def _persist_spy(**kwargs) -> None:
        seen_calls.append(kwargs)
        original_persist(**kwargs)

    monkeypatch.setattr(svc, "_persist_incremental_store_changes", _persist_spy)
    monkeypatch.setattr(
        svc,
        "_save_json",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("legacy full rebuild path should not run")),
    )

    p = Path(test_config["ai_material_dir"]) / "math" / "post_startup_new.pdf"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"fake-pdf-content")

    svc._bootstrap_sync_sync()

    assert len(seen_calls) == 1
    assert "math/post_startup_new.pdf" in seen_calls[0]["index_upserts"]
    index_by_rel, metadata_by_rel, vector_by_rel = svc._load_incremental_store_maps()
    assert "math/post_startup_new.pdf" in index_by_rel
    assert "math/post_startup_new.pdf" in metadata_by_rel
    assert "math/post_startup_new.pdf" in vector_by_rel


def test_bootstrap_handles_empty_material_dir(monkeypatch) -> None:
    svc = _new_service(monkeypatch)
    monkeypatch.setattr(svc, "_auto_organize_materials_on_boot", lambda _index_list: ({}, {}, set()))

    svc._bootstrap_quick_sync_sync()
    svc._bootstrap_sync_sync()

    index_by_rel, metadata_by_rel, vector_by_rel = svc._load_incremental_store_maps()
    assert index_by_rel == {}
    assert metadata_by_rel == {}
    assert vector_by_rel == {}
    assert svc._semantic_active_count == 0
