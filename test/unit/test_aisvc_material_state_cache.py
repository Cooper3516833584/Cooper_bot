from __future__ import annotations

from pathlib import Path

from aisvc import AIService


class _DummyLog:
    def info(self, _msg: str) -> None:
        return

    def warning(self, _msg: str) -> None:
        return


def _new_service() -> AIService:
    return AIService(log=_DummyLog())


def _write_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def test_material_state_cache_save_and_load_roundtrip() -> None:
    svc = _new_service()

    cache_map = {
        "math/ok.pdf": {
            "size": 12,
            "mtime_ns": 1000,
            "sha256": "a" * 64,
        },
        "bad/hash.pdf": {
            "size": 5,
            "mtime_ns": 1,
            "sha256": "not-a-hash",
        },
    }
    svc._save_material_state_cache(cache_map)

    loaded = svc._load_material_state_cache()
    assert loaded == {
        "math/ok.pdf": {
            "size": 12,
            "mtime_ns": 1000,
            "sha256": "a" * 64,
        }
    }


def test_state_cache_reuses_hash_when_size_and_mtime_unchanged(monkeypatch, test_config: dict) -> None:
    svc = _new_service()
    path = Path(test_config["ai_material_dir"]) / "math" / "cache_hit.pdf"
    _write_file(path, b"content-v1")

    rel = "math/cache_hit.pdf"
    size, mtime_ns = svc._file_stat_signature(path)
    sha = svc._file_sha256(path)
    cache_map = {rel: {"size": size, "mtime_ns": mtime_ns, "sha256": sha}}

    def _hash_should_not_run(_path: Path) -> str:
        raise AssertionError("hash recomputation should not happen for unchanged file")

    monkeypatch.setattr(svc, "_file_sha256", _hash_should_not_run)
    got, updated = svc._get_file_hash_by_state_cache(path, rel, cache_map)

    assert got == sha
    assert updated is False


def test_state_cache_recomputes_hash_when_file_changed(test_config: dict) -> None:
    svc = _new_service()
    path = Path(test_config["ai_material_dir"]) / "math" / "cache_miss.pdf"
    _write_file(path, b"v1")

    rel = "math/cache_miss.pdf"
    old_size, old_mtime_ns = svc._file_stat_signature(path)
    old_sha = svc._file_sha256(path)
    cache_map = {rel: {"size": old_size, "mtime_ns": old_mtime_ns, "sha256": old_sha}}

    _write_file(path, b"v2-new-content")
    got, updated = svc._get_file_hash_by_state_cache(path, rel, cache_map)

    assert updated is True
    assert got != old_sha
    assert cache_map[rel]["sha256"] == got


def test_set_file_hash_state_cache_entry_is_idempotent(test_config: dict) -> None:
    svc = _new_service()
    path = Path(test_config["ai_material_dir"]) / "math" / "set_cache.pdf"
    _write_file(path, b"same")
    rel = "math/set_cache.pdf"
    sha = svc._file_sha256(path)

    cache_map: dict[str, dict] = {}
    changed_first = svc._set_file_hash_state_cache_entry(path, rel, sha, cache_map)
    changed_second = svc._set_file_hash_state_cache_entry(path, rel, sha, cache_map)

    assert changed_first is True
    assert changed_second is False
