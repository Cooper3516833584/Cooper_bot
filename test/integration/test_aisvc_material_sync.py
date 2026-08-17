from __future__ import annotations

from pathlib import Path
from typing import Any

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


def _write_material_file(material_dir: Path, rel: str, *, content: bytes = b"fake-pdf") -> Path:
    p = material_dir / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(content)
    return p


def _seed_store_entry(
    svc: AIService,
    rel: str,
    *,
    summary: str = "seed-summary",
    vec: tuple[float, ...] = (0.1, 0.2, 0.3),
) -> None:
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
        vector_upserts={rel_norm: np.asarray(vec, dtype=np.float64)},
        vector_deletes=set(),
    )


def _install_pipeline(
    monkeypatch,
    svc: AIService,
    *,
    summary_prefix: str = "pipeline",
    vector: tuple[float, ...] = (1.0, 0.0, 0.0),
    hint_capture: dict[str, Any] | None = None,
) -> None:
    def _fake_pipeline(rel: str, hint=None, build_vector: bool = True) -> dict:
        _ = build_vector
        rel_norm = svc._normalize_rel(rel)
        if hint_capture is not None:
            hint_capture[rel_norm] = hint
        subject = svc._subject_from_rel(rel_norm)
        filename = Path(rel_norm).name
        return {
            "index_item": {
                "file_path": svc._to_store_rel(rel_norm),
                "subject": subject,
                "filename": filename,
                "file_type": "pdf",
                "keywords": [subject, "sync"],
                "summary": f"{summary_prefix}:{filename}",
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
        lambda _text: np.asarray(vector, dtype=np.float64),
    )


def test_material_sync_add_new_file(monkeypatch, test_config: dict) -> None:
    svc = _new_service(monkeypatch)
    _install_pipeline(monkeypatch, svc, summary_prefix="add", vector=(0.9, 0.1, 0.0))
    monkeypatch.setattr(svc, "_auto_organize_materials_on_boot", lambda _index_list: ({}, {}, set()))

    _write_material_file(Path(test_config["ai_material_dir"]), "math/add_case.pdf")
    svc._bootstrap_sync_sync()

    index_by_rel, metadata_by_rel, vector_by_rel = svc._load_incremental_store_maps()
    assert "math/add_case.pdf" in index_by_rel
    assert "math/add_case.pdf" in metadata_by_rel
    assert "math/add_case.pdf" in vector_by_rel
    assert "math/add_case.pdf" in svc._semantic_entry_by_rel


def test_material_sync_delete_removed_file(monkeypatch) -> None:
    svc = _new_service(monkeypatch)
    _seed_store_entry(svc, "math/delete_case.pdf", summary="to-delete")
    monkeypatch.setattr(svc, "_auto_organize_materials_on_boot", lambda _index_list: ({}, {}, set()))
    monkeypatch.setattr(
        svc,
        "_run_new_file_pipeline",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("delete-only sync should not build new pipeline")),
    )

    svc._bootstrap_sync_sync()

    index_by_rel, metadata_by_rel, vector_by_rel = svc._load_incremental_store_maps()
    assert "math/delete_case.pdf" not in index_by_rel
    assert "math/delete_case.pdf" not in metadata_by_rel
    assert "math/delete_case.pdf" not in vector_by_rel


def test_material_sync_move_updates_rel_mapping(monkeypatch, test_config: dict) -> None:
    svc = _new_service(monkeypatch)
    old_rel = "math/move_old.pdf"
    new_rel = "physics/move_new.pdf"
    _seed_store_entry(svc, old_rel, summary="before-move", vec=(0.2, 0.2, 0.2))
    _write_material_file(Path(test_config["ai_material_dir"]), new_rel)

    monkeypatch.setattr(svc, "_auto_organize_materials_on_boot", lambda _index_list: ({old_rel: new_rel}, {}, set()))
    monkeypatch.setattr(
        svc,
        "_run_new_file_pipeline",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("pure move should not require pipeline")),
    )

    svc._bootstrap_sync_sync()

    index_by_rel, metadata_by_rel, vector_by_rel = svc._load_incremental_store_maps()
    assert old_rel not in index_by_rel
    assert old_rel not in metadata_by_rel
    assert old_rel not in vector_by_rel
    assert new_rel in index_by_rel
    assert new_rel in metadata_by_rel
    assert new_rel in vector_by_rel
    assert metadata_by_rel[new_rel]["subject"] == "physics"


def test_material_sync_modify_rebuilds_index_and_vector(monkeypatch, test_config: dict) -> None:
    svc = _new_service(monkeypatch)
    rel = "math/modify_case.pdf"
    _seed_store_entry(svc, rel, summary="old-summary", vec=(0.1, 0.2, 0.3))
    _write_material_file(Path(test_config["ai_material_dir"]), rel, content=b"changed-content")

    _install_pipeline(monkeypatch, svc, summary_prefix="modified", vector=(0.0, 5.0, 0.0))
    monkeypatch.setattr(svc, "_auto_organize_materials_on_boot", lambda _index_list: ({}, {}, {rel}))

    svc._bootstrap_sync_sync()

    index_by_rel, _metadata_by_rel, vector_by_rel = svc._load_incremental_store_maps()
    assert index_by_rel[rel]["summary"] == "modified:modify_case.pdf"
    assert np.allclose(vector_by_rel[rel], np.asarray([0.0, 5.0, 0.0], dtype=np.float64))
    assert rel in svc._semantic_entry_by_rel


def test_material_sync_tbd_to_subject_uses_hint_and_persists(monkeypatch, test_config: dict) -> None:
    svc = _new_service(monkeypatch)
    hint_capture: dict[str, Any] = {}
    _install_pipeline(monkeypatch, svc, summary_prefix="tbd", vector=(0.3, 0.3, 0.3), hint_capture=hint_capture)

    new_rel = "chemistry/from_tbd.pdf"
    _write_material_file(Path(test_config["ai_material_dir"]), new_rel, content=b"moved-from-tbd")
    hint_payload = {
        "from_tbd": True,
        "old_rel": "TBD/from_tbd.pdf",
        "classified_target": "chemistry",
        "snippet": "snippet from tbd classify",
    }
    monkeypatch.setattr(
        svc,
        "_auto_organize_materials_on_boot",
        lambda _index_list: ({"TBD/from_tbd.pdf": new_rel}, {new_rel: hint_payload}, set()),
    )

    svc._bootstrap_sync_sync()

    assert new_rel in hint_capture
    assert isinstance(hint_capture[new_rel], dict)
    assert hint_capture[new_rel]["from_tbd"] is True
    assert hint_capture[new_rel]["classified_target"] == "chemistry"

    index_by_rel, metadata_by_rel, vector_by_rel = svc._load_incremental_store_maps()
    assert new_rel in index_by_rel
    assert new_rel in metadata_by_rel
    assert new_rel in vector_by_rel
    assert new_rel in svc._semantic_entry_by_rel
