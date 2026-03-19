from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Mapping, Sequence


def ensure_dir(path: Path) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_text_file(path: Path, text: str = "sample", *, encoding: str = "utf-8") -> Path:
    path = Path(path)
    ensure_dir(path.parent)
    path.write_text(str(text), encoding=encoding)
    return path


def write_json_file(path: Path, payload: Mapping[str, Any], *, indent: int = 2) -> Path:
    path = Path(path)
    ensure_dir(path.parent)
    path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=indent), encoding="utf-8")
    return path


def write_fake_file(path: Path, *, size: int = 64, fill_byte: bytes = b"x") -> Path:
    if not fill_byte or len(fill_byte) != 1:
        raise ValueError("fill_byte must be a single byte.")
    path = Path(path)
    ensure_dir(path.parent)
    path.write_bytes(fill_byte * max(1, int(size)))
    return path


def build_material_tree(data_dir: Path) -> dict[str, Any]:
    """Build a small sample textbook/material tree."""
    data_dir = Path(data_dir)
    material_root = ensure_dir(data_dir / "public" / "textbook_and_material")
    tbd_dir = ensure_dir(material_root / "TBD")

    files = [
        write_text_file(material_root / "math" / "calculus_intro.txt", "limit, continuity, derivative"),
        write_text_file(material_root / "signals" / "sampling_theorem.md", "Nyquist sampling theorem"),
        write_fake_file(tbd_dir / "new_upload.pdf", size=128),
        write_fake_file(tbd_dir / "reading_list.epub", size=96),
    ]

    write_json_file(
        material_root / "all_files_index.json",
        {
            "generated_by": "test.helpers.sample_builders",
            "files": [str(p.relative_to(material_root)).replace("\\", "/") for p in files],
        },
    )

    return {
        "material_root": material_root,
        "tbd_dir": tbd_dir,
        "files": files,
    }


def build_find_tree(data_dir: Path) -> dict[str, Any]:
    """Build a sample tree suitable for /find tests."""
    data_dir = Path(data_dir)
    public_root = ensure_dir(data_dir / "public")
    friend_root = ensure_dir(data_dir / "friend")
    groups_root = ensure_dir(data_dir / "groups" / "20001")

    files = [
        write_text_file(public_root / "math" / "linear_algebra" / "week1_notes.txt", "matrix rank"),
        write_text_file(public_root / "physics" / "lab" / "experiment_guide.md", "oscilloscope setup"),
        write_fake_file(friend_root / "english" / "cet4_words.pdf", size=128),
        write_fake_file(groups_root / "notice" / "week3_schedule.docx", size=96),
    ]

    return {
        "find_root_candidates": [public_root, friend_root, groups_root],
        "files": files,
        "keywords": ["matrix", "guide", "schedule"],
    }


def _build_handin_task(
    *,
    task_id: str,
    task_name: str,
    group_id: int,
    creator_id: int,
    deadline_ts: float,
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "group_id": int(group_id),
        "creator_id": int(creator_id),
        "name": str(task_name),
        "created_ts": float(time.time()),
        "remind_ts_list": [],
        "remind_sent_idx": 0,
        "deadline_ts": float(deadline_ts),
        "deadline_sent": False,
        "closed": False,
        "cancelled": False,
        "cancelled_ts": 0.0,
        "cancelled_by": 0,
        "last_handinget_ts": 0.0,
        "purged": False,
        "purged_ts": 0.0,
    }


def build_handin_env(
    data_dir: Path,
    *,
    group_id: int = 20001,
    creator_id: int = 900001,
    submitter_ids: Sequence[int] = (10001,),
    task_id: str = "task_demo",
    task_name: str = "demo_handin",
) -> dict[str, Any]:
    """Build a minimal handin environment with db + files + inbox."""
    data_dir = Path(data_dir)
    handin_root = ensure_dir(data_dir / "handin" / str(int(group_id)) / str(task_name) / "files")
    users_root = ensure_dir(data_dir / "users")
    inbox_root = ensure_dir(users_root / "_handin_inbox")
    handin_db_path = data_dir / "_handin_tasks.json"

    submitted_files: list[Path] = []
    for uid in submitter_ids:
        submitted_files.append(
            write_fake_file(
                handin_root / f"U{int(uid):08d}_sample_submission.pdf",
                size=128,
            )
        )
        write_text_file(
            inbox_root / str(int(uid)) / "pending_upload.txt",
            "pending file for submit flow tests",
        )

    task_payload = _build_handin_task(
        task_id=str(task_id),
        task_name=str(task_name),
        group_id=int(group_id),
        creator_id=int(creator_id),
        deadline_ts=time.time() + 7 * 24 * 60 * 60,
    )
    write_json_file(handin_db_path, {str(task_id): task_payload})

    return {
        "handin_root": handin_root,
        "inbox_root": inbox_root,
        "handin_db_path": handin_db_path,
        "task": task_payload,
        "submitted_files": submitted_files,
    }
