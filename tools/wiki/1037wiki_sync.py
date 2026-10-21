"""Manual, AI-agent-friendly 1037wiki <-> local material library sync.

This CLI is deliberately standalone: it does not import or modify the running
bot.  All write operations are dry-run unless ``--execute`` is passed.

Typical use::

    python tools/wiki/1037wiki_sync.py inventory --refresh --json
    python tools/wiki/1037wiki_sync.py upload --scope indexed --json
    python tools/wiki/1037wiki_sync.py download --json
    python tools/wiki/1037wiki_sync.py upload --execute --limit 5
    python tools/wiki/1037wiki_sync.py download --execute --limit 20
"""
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import re
import shutil
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import importlib

api = importlib.import_module("1037wiki_api")
plan_mod = importlib.import_module("build_1037wiki_upload_plan")

REPO = HERE.parents[1]
ROOT = REPO / "storage" / "documents" / "public" / "textbook_and_material"
DB = REPO / "runtime" / "databases" / "ai" / "semantic_store.sqlite3"
STATE_DIR = REPO / "runtime" / "state" / "wiki"
REMOTE_CACHE = STATE_DIR / "1037wiki_remote_inventory.json"
HASH_CACHE = STATE_DIR / "1037wiki_local_md5_cache.json"
SYNC_STATE = STATE_DIR / "1037wiki_sync_state.json"
SYNC_PLAN = STATE_DIR / "1037wiki_sync_plan.json"
REPORT = STATE_DIR / "1037wiki_sync_report.json"

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(errors="replace")
    except Exception:
        pass

DOC_EXTS = {
    ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".pptm",
    ".xls", ".xlsx", ".xlsm", ".txt", ".md", ".zip", ".rar", ".7z",
}
COPYRIGHT_MARKERS = (
    "Z-Library", "z-library", "教材", "课本", "第六版", "第七版",
    "中文版", "出版社",
)
OWN_NAMES = {"cooper_chen", "cooper_bot"}
LOCAL_SUBJECT_ALIASES = {
    "习思想_习概": {"习概", "习近平新时代社会主义思想概论", "习近平新时代中国特色社会主义思想概论"},
    "信号与系统": {"信号"},
    "马克思主义基本原理": {"马原"},
    "毛泽东思想和中国特色社会主义理论体系概论": {"毛概"},
    "数电": {"数字电子技术基础", "数字电路"},
    "模电": {"模拟电子技术", "模电"},
    "大学物理": {"大物"},
    "复变函数": {"复变函数与积分变换", "复变"},
    "概率论与数理统计": {"概率论"},
    "数据库": {"数据库系统原理"},
    "操作系统": {"操作系统设计原理"},
    "数理方程": {"数学物理方程与特殊函数"},
    "思想政治": {"思政"},
    "转专业与启明选拔": {"启明及转专业", "转专业", "启明"},
}


def log(message: Any, *, json_mode: bool = False) -> None:
    print(str(message), file=sys.stderr if json_mode else sys.stdout, flush=True)


def norm_title(value: object) -> str:
    return re.sub(r"\s+", "", str(value or "")).casefold()


def copyright_risk(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(marker.lower() in lowered for marker in COPYRIGHT_MARKERS)


def load_index_meta() -> dict[str, dict[str, Any]]:
    meta: dict[str, dict[str, Any]] = {}
    if not DB.exists():
        return meta
    conn = sqlite3.connect(str(DB))
    try:
        for rel, payload in conn.execute("SELECT rel, payload FROM index_items ORDER BY rel"):
            try:
                meta[str(rel)] = json.loads(payload)
            except Exception:
                continue
    finally:
        conn.close()
    return meta


def iter_local_files(scope: str, meta: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if scope == "indexed":
        candidates = [ROOT / rel for rel in meta]
    else:
        candidates = [p for p in ROOT.rglob("*") if p.is_file()]
    for path in candidates:
        if not path.is_file():
            continue
        try:
            rel = path.relative_to(ROOT).as_posix()
        except ValueError:
            continue
        parts = Path(rel).parts
        if any(part.startswith(".") or part == "__pycache__" for part in parts):
            continue
        if parts and parts[0] == "TBD":
            continue
        if scope == "documents" and path.suffix.lower() not in DOC_EXTS:
            continue
        item = meta.get(rel) or {}
        rows.append(
            {
                "rel": rel,
                "path": path,
                "subject": parts[0] if parts else "未分类",
                "filename": path.name,
                "size": path.stat().st_size,
                "ext": path.suffix.lower(),
                "summary": str(item.get("summary") or ""),
                "keywords": [str(x) for x in (item.get("keywords") or []) if str(x).strip()],
            }
        )
    rows.sort(key=lambda x: x["rel"].casefold())
    return rows


def load_hash_cache() -> dict[str, Any]:
    data = api.read_json(HASH_CACHE, {"files": {}})
    if not isinstance(data, dict) or not isinstance(data.get("files"), dict):
        return {"files": {}}
    return data


def local_md5(row: dict[str, Any], cache: dict[str, Any]) -> str:
    rel = row["rel"]
    path = row["path"]
    stat = path.stat()
    old = cache["files"].get(rel)
    if isinstance(old, dict) and old.get("size") == stat.st_size and old.get("mtime_ns") == stat.st_mtime_ns:
        if old.get("md5"):
            return str(old["md5"])
    digest = api.md5_file(path)
    cache["files"][rel] = {
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "md5": digest,
    }
    return digest


def save_hash_cache(cache: dict[str, Any]) -> None:
    api.atomic_write_json(HASH_CACHE, cache)


def remote_index(inventory: dict[str, Any]) -> dict[str, Any]:
    md5s: dict[str, list[dict[str, Any]]] = {}
    sizes: set[int] = set()
    titles: set[str] = set()
    all_files: list[dict[str, Any]] = []
    for page in inventory.get("pages") or []:
        title = str(page.get("title") or "")
        titles.add(norm_title(title))
        for f in page.get("files") or []:
            if not isinstance(f, dict):
                continue
            record = {
                **f,
                "page_id": page.get("id"),
                "page_title": title,
                "category_name": page.get("category_name"),
                "category_parent": page.get("category_parent"),
                "is_me": bool(page.get("is_me")),
            }
            all_files.append(record)
            size = int(f.get("size") or 0)
            if size:
                sizes.add(size)
            md5 = str(f.get("md5") or "").lower()
            if md5:
                md5s.setdefault(md5, []).append(record)
    return {"md5": md5s, "sizes": sizes, "titles": titles, "files": all_files}


def build_groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        parts = Path(row["rel"]).parts
        subject = row["subject"]
        if len(parts) <= 1:
            category = plan_mod.COURSE_MATERIAL
        elif len(parts) == 2:
            combined = " ".join(
                [row["rel"], row["filename"], " ".join(row["keywords"]), row["summary"]]
            )
            category = plan_mod.guess_category(combined, row["ext"])
        else:
            category = parts[1]
        groups.setdefault((subject, category), []).append(row)

    pages: list[dict[str, Any]] = []
    seen_titles: dict[str, int] = {}
    for (subject, category), files in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1])):
        files = sorted(files, key=lambda x: x["rel"])
        title = plan_mod.safe_title(subject, category, files)
        key = norm_title(title)
        seen_titles[key] = seen_titles.get(key, 0) + 1
        if seen_titles[key] > 1:
            title = f"{title} ({seen_titles[key]})"
        tags: list[str] = []
        for tag in [subject, category] + [
            keyword for keyword, _count in plan_mod.Counter(
                k for f in files for k in f["keywords"]
            ).most_common(8)
        ]:
            tag = plan_mod.clean_tag(tag)
            if tag and tag not in tags:
                tags.append(tag)
        pages.append(
            {
                "title": title,
                "parent_category": "课程",
                "subject": subject,
                "material_category": category,
                "tags": tags[:10],
                "description": plan_mod.build_description(subject, category, files),
                "files": files,
            }
        )
    return pages


def plan_upload(
    pages: list[dict[str, Any]],
    remote: dict[str, Any],
    hash_cache: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    actions: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    state = api.read_json(SYNC_STATE, {"pages": {}})
    uploaded_titles = {norm_title(t) for t in (state.get("pages") or {})}
    counts = {
        "pages_total": len(pages),
        "pages_ready": 0,
        "pages_skip_duplicate": 0,
        "pages_skip_partial": 0,
        "pages_skip_title": 0,
        "pages_skip_copyright": 0,
        "pages_skip_uploaded": 0,
        "pages_skip_large": 0,
        "files_ready": 0,
        "files_already_remote": 0,
        "files_skipped_large": 0,
        "bytes_ready": 0,
    }
    for page in pages:
        subject_filter = getattr(args, "subject", "") or ""
        title_filter = getattr(args, "title", "") or ""
        if subject_filter and subject_filter.casefold() not in str(page["subject"]).casefold():
            continue
        if title_filter and title_filter != page["title"]:
            continue
        if norm_title(page["title"]) in uploaded_titles and not getattr(args, "force", False):
            counts["pages_skip_uploaded"] += 1
            skipped.append({"title": page["title"], "reason": "already_uploaded_by_sync"})
            continue
        risk_text = " ".join(
            [
                str(page.get("title") or ""),
                str(page.get("description") or ""),
                " ".join(map(str, page.get("tags") or [])),
                " ".join(str(f["filename"]) for f in page["files"]),
                " ".join(str(f.get("summary") or "") for f in page["files"]),
            ]
        )
        if not args.include_copyright_risk and copyright_risk(risk_text):
            counts["pages_skip_copyright"] += 1
            skipped.append({"title": page["title"], "reason": "copyright_risk"})
            continue
        if norm_title(page["title"]) in remote["titles"] and not args.allow_existing_title:
            counts["pages_skip_title"] += 1
            skipped.append({"title": page["title"], "reason": "existing_title"})
            continue

        missing: list[dict[str, Any]] = []
        for item in page["files"]:
            if item["size"] > api.MAX_UPLOAD_BYTES:
                counts["files_skipped_large"] += 1
                continue
            md5: str | None = None
            if item["size"] in remote["sizes"]:
                md5 = local_md5(item, hash_cache)
                if md5 in remote["md5"]:
                    counts["files_already_remote"] += 1
                    continue
            missing.append({**item, "md5": md5})

        if not missing:
            counts["pages_skip_duplicate"] += 1
            skipped.append({"title": page["title"], "reason": "all_files_already_remote"})
            continue
        if len(missing) < len(page["files"]) and not args.allow_partial:
            counts["pages_skip_partial"] += 1
            skipped.append(
                {
                    "title": page["title"],
                    "reason": "partially_present",
                    "remote_count": len(page["files"]) - len(missing),
                    "missing_count": len(missing),
                }
            )
            continue
        if not missing:
            counts["pages_skip_large"] += 1
            skipped.append({"title": page["title"], "reason": "all_files_too_large"})
            continue

        description = str(page["description"])
        if len(missing) < len(page["files"]):
            description = (description + "（补充本地新增/缺失文件）")[:500]
        action = {
            "title": page["title"],
            "parent_category": page["parent_category"],
            "subject": page["subject"],
            "material_category": page["material_category"],
            "tags": page["tags"],
            "description": description,
            "files": [
                {
                    "rel": f["rel"],
                    "path": str(f["path"]),
                    "filename": f["filename"],
                    "size": f["size"],
                    "md5": f["md5"],
                }
                for f in missing
            ],
        }
        actions.append(action)
        counts["pages_ready"] += 1
        counts["files_ready"] += len(missing)
        counts["bytes_ready"] += sum(f["size"] for f in missing)

    if args.limit is not None:
        actions = actions[: int(args.limit)]
    counts["pages_ready"] = len(actions)
    counts["files_ready"] = sum(len(a["files"]) for a in actions)
    counts["bytes_ready"] = sum(int(f["size"]) for a in actions for f in a["files"])
    plan = {"counts": counts, "actions": actions, "skipped": skipped}
    return plan


def filter_available_categories(plan: dict[str, Any], token: str) -> dict[str, Any]:
    """Drop pages whose category does not exist and cannot be created by this account."""
    resolver = CategoryResolver(token)
    kept: list[dict[str, Any]] = []
    counts = plan["counts"]
    counts.setdefault("pages_skip_missing_category", 0)
    for action in plan.get("actions") or []:
        try:
            category_id = resolver.find(
                str(action.get("subject") or ""),
                [str(x) for x in (action.get("tags") or []) if str(x).strip()],
            )
        except Exception:
            category_id = None
        if not category_id:
            counts["pages_skip_missing_category"] += 1
            plan.setdefault("skipped", []).append(
                {"title": action.get("title"), "reason": "category_missing", "subject": action.get("subject")}
            )
            continue
        action["category_id"] = category_id
        kept.append(action)
    plan["actions"] = kept
    counts["pages_ready"] = len(kept)
    counts["files_ready"] = sum(len(a.get("files") or []) for a in kept)
    counts["bytes_ready"] = sum(int(f.get("size") or 0) for a in kept for f in a.get("files") or [])
    return plan


def upload_file(token: str, path: Path) -> tuple[str, bool]:
    if path.stat().st_size > api.MAX_UPLOAD_BYTES:
        raise api.WikiApiError(f"file exceeds 200MB: {path}")
    md5 = api.md5_file(path)
    pre = api.api_request(
        token, "POST", "/file/upload/url", data={"file_name": path.name, "md5": md5}
    )
    if isinstance(pre, dict) and pre.get("file_id"):
        return str(pre["file_id"]), True
    if not isinstance(pre, dict) or not pre.get("upload_url") or not pre.get("file_name"):
        raise api.WikiApiError(f"unexpected upload-url response for {path.name}")
    content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    data = path.read_bytes()
    req = urllib.request.Request(
        str(pre["upload_url"]),
        data=data,
        headers={"Content-Type": content_type, "User-Agent": "Mozilla/5.0"},
        method="PUT",
    )
    try:
        with urllib.request.urlopen(req, timeout=max(60, len(data) / 1024 / 1024 * 20)) as response:
            if response.status < 200 or response.status >= 300:
                raise api.WikiApiError(f"S3 upload failed: {response.status}")
    except urllib.error.HTTPError as exc:
        raise api.WikiApiError(f"S3 upload failed: HTTP {exc.code}") from exc
    file_id = api.api_request(
        token, "POST", "/file/upload/finish", data={"file_name": pre["file_name"]}
    )
    if not file_id:
        raise api.WikiApiError(f"missing file_id after upload finish for {path.name}")
    return str(file_id), False


class CategoryResolver:
    def __init__(self, token: str):
        self.token = token
        self._items: list[dict[str, Any]] | None = None

    def _matches(self, item: dict[str, Any], candidates: set[str]) -> bool:
        names = {str(item.get("name") or "").strip()}
        names.update(str(x).strip() for x in (item.get("aliases") or []))
        return bool(candidates & names)

    def _search(self, term: str, candidates: set[str]) -> str | None:
        data = api.api_request(
            self.token,
            "GET",
            "/category/list",
            params={"search": term, "parent": "课程", "page": 1, "page_size": 20},
        )
        for item in (data or {}).get("items") or []:
            if isinstance(item, dict) and item.get("parent") == "课程" and self._matches(item, candidates):
                return str(item.get("id") or "") or None
        return None

    @staticmethod
    def _fuzzy_terms(name: str, aliases: Iterable[str]) -> list[str]:
        terms: set[str] = set()
        for base in [name, *aliases]:
            base = str(base or "").strip()
            if not base:
                continue
            terms.add(base)
            for size in range(2, min(7, len(base)) + 1):
                for start in range(0, len(base) - size + 1):
                    terms.add(base[start:start + size])
        return sorted(terms, key=lambda x: (-len(x), x))

    def find(self, name: str, aliases: Iterable[str] = ()) -> str | None:
        candidates = {str(name or "").strip()}
        candidates.update(str(x).strip() for x in aliases if str(x).strip())
        for term in [name, *aliases]:
            if term:
                found = self._search(str(term), candidates)
                if found:
                    return found
        for term in self._fuzzy_terms(name, aliases):
            found = self._search(term, candidates)
            if found:
                return found
        return None

    def _load(self) -> list[dict[str, Any]]:
        if self._items is not None:
            return self._items
        items: list[dict[str, Any]] = []
        page = 1
        while True:
            data = api.api_request(
                self.token,
                "GET",
                "/category/list",
                params={"parent": "课程", "page": page, "page_size": 20},
            )
            batch = (data or {}).get("items") or []
            items.extend(x for x in batch if isinstance(x, dict))
            paging = (data or {}).get("paging") or {}
            if not paging.get("has_more"):
                break
            page += 1
        self._items = items
        return items

    def get_or_create(self, name: str, aliases: Iterable[str]) -> str:
        found = self.find(name, aliases)
        if found:
            return found
        for item in self._load():
            if item.get("parent") == "课程" and self._matches(
                item, {str(name or "").strip(), *[str(x).strip() for x in aliases if str(x).strip()]}
            ):
                return str(item["id"])
        try:
            created = api.api_request(
                self.token,
                "POST",
                "/category",
                data={"name": name, "parent": "课程", "aliases": list(aliases)[:10]},
            )
            if isinstance(created, dict) and created.get("id"):
                self._items = None
                return str(created["id"])
        except api.WikiApiError:
            found = self.find(name, aliases)
            if found:
                return found
            raise
        found = self.find(name, aliases)
        if found:
            return found
        raise api.WikiApiError(f"missing category id for {name}")


def execute_upload(plan: dict[str, Any], token: str, state: dict[str, Any]) -> dict[str, Any]:
    resolver = CategoryResolver(token)
    results = {"pages_done": 0, "files_uploaded": 0, "files_reused": 0, "errors": []}
    lock = threading.Lock()
    for action in plan.get("actions") or []:
        title = action["title"]
        try:
            category_id = str(action.get("category_id") or "") or resolver.get_or_create(
                action["subject"], action.get("tags") or []
            )
            file_ids: list[str] = []
            owned_ids: list[str] = []
            refs: list[dict[str, Any]] = []
            for item in action["files"]:
                rel = item["rel"]
                path = Path(item["path"])
                want_md5 = item.get("md5") or api.md5_file(path)
                cached = (state.get("files") or {}).get(rel)
                if (
                    isinstance(cached, dict)
                    and cached.get("md5") == want_md5
                    and cached.get("id")
                ):
                    file_id = str(cached["id"])
                    reused = bool(cached.get("reused"))
                else:
                    file_id, reused = upload_file(token, path)
                    state.setdefault("files", {})[rel] = {
                        "id": file_id,
                        "md5": want_md5,
                        "reused": reused,
                        "ts": int(time.time()),
                    }
                    api.atomic_write_json(SYNC_STATE, state)
                file_ids.append(file_id)
                refs.append({"rel": rel, "id": file_id, "reused": reused})
                if not reused:
                    owned_ids.append(file_id)
                if reused:
                    results["files_reused"] += 1
                else:
                    results["files_uploaded"] += 1
            payload = {
                "title": title,
                "tags": action.get("tags") or [],
                "description": action.get("description") or title,
                "category_id": category_id,
                "collection_ids": None,
                "anonymous": False,
                "file_ids": file_ids,
            }
            try:
                page_id = api.api_request(token, "POST", "/page", data=payload, timeout=60)
            except api.WikiApiError as exc:
                if "403" not in str(exc) or "无权限使用该文件" not in str(exc):
                    raise
                if not owned_ids:
                    raise
                payload["file_ids"] = owned_ids
                page_id = api.api_request(token, "POST", "/page", data=payload, timeout=60)
                file_ids = owned_ids
            state.setdefault("pages", {})[title] = {
                "page_id": str(page_id),
                "category_id": category_id,
                "file_ids": file_ids,
                "ts": int(time.time()),
            }
            state["errors"] = [
                error for error in state.get("errors", []) if error.get("title") != title
            ]
            api.atomic_write_json(SYNC_STATE, state)
            results["pages_done"] += 1
            log(f"[upload] page ok: {title} ({len(file_ids)} files)")
        except Exception as exc:
            results["errors"].append({"title": title, "error": str(exc)})
            state.setdefault("errors", []).append(
                {"title": title, "error": str(exc), "ts": int(time.time())}
            )
            api.atomic_write_json(SYNC_STATE, state)
            log(f"[upload] page failed: {title}: {exc}")
    return results


def resolve_subject_dir(category_name: str, category_parent: str) -> str:
    name = api.sanitize_component(category_name or "未分类")
    if (ROOT / name).is_dir():
        return name
    folded = str(category_name or "").strip().casefold()
    for local_name, aliases in LOCAL_SUBJECT_ALIASES.items():
        if (ROOT / local_name).is_dir() and (
            folded == local_name.casefold()
            or folded in {a.casefold() for a in aliases}
        ):
            return local_name
    if category_parent == "生活":
        return f"生活资料/{name}"
    return name


def infer_subject_from_title(title: str) -> str:
    text = str(title or "").strip()
    if not text:
        return "未分类"
    candidates: list[str] = []
    for path in ROOT.iterdir() if ROOT.is_dir() else []:
        if not path.is_dir() or path.name.startswith("."):
            continue
        name = path.name
        if text.startswith(name) or name in text:
            candidates.append(name)
    for local_name, aliases in LOCAL_SUBJECT_ALIASES.items():
        if not (ROOT / local_name).is_dir():
            continue
        if any(text.startswith(alias) or alias in text for alias in aliases):
            candidates.append(local_name)
    return max(candidates, key=len) if candidates else "未分类"


def destination_for(page: dict[str, Any], remote_file: dict[str, Any]) -> Path:
    category_name = str(page.get("category_name") or "").strip()
    if category_name:
        subject_dir = resolve_subject_dir(category_name, str(page.get("category_parent") or ""))
    else:
        subject_dir = infer_subject_from_title(str(page.get("title") or ""))
    page_dir = api.sanitize_component(page.get("title") or page.get("id") or "未命名页面")
    filename = api.sanitize_component(remote_file.get("filename") or remote_file.get("id") or "file")
    return ROOT / subject_dir / page_dir / filename


def unique_destination(path: Path, reserved: set[str], remote_md5: str | None) -> tuple[Path, bool]:
    if path.as_posix().casefold() not in reserved and not path.exists():
        reserved.add(path.as_posix().casefold())
        return path, False
    if path.exists():
        try:
            if remote_md5 and api.md5_file(path) == remote_md5:
                reserved.add(path.as_posix().casefold())
                return path, True
        except Exception:
            pass
    stem, suffix = path.stem, path.suffix
    for index in range(1, 1000):
        tag = " (1037wiki)" if index == 1 else f" (1037wiki {index})"
        candidate = path.with_name(f"{stem}{tag}{suffix}")
        key = candidate.as_posix().casefold()
        if key not in reserved and not candidate.exists():
            reserved.add(key)
            return candidate, False
    raise api.WikiApiError(f"too many filename collisions: {path}")


def plan_download(
    inventory: dict[str, Any],
    remote: dict[str, Any],
    hash_cache: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    local_by_size: dict[int, list[dict[str, Any]]] = {}
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        try:
            rel = path.relative_to(ROOT).as_posix()
        except ValueError:
            continue
        if any(part.startswith(".") for part in Path(rel).parts):
            continue
        local_by_size.setdefault(path.stat().st_size, []).append(
            {"rel": rel, "path": path, "filename": path.name, "size": path.stat().st_size}
        )

    local_md5_sets: dict[int, set[str]] = {}
    actions: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    counts = {
        "pages_total": len(inventory.get("pages") or []),
        "pages_selected": 0,
        "files_total": 0,
        "files_already_local": 0,
        "files_ready": 0,
        "bytes_ready": 0,
        "pages_skip_own": 0,
    }
    reserved: set[str] = set()
    max_files = getattr(args, "max_files", None)
    max_bytes = int(args.max_mb * 1024 * 1024) if getattr(args, "max_mb", None) else None
    page_limit = getattr(args, "limit", None)
    include_own = bool(getattr(args, "include_own", False))
    category_filter = getattr(args, "category", "") or ""
    page_id_filter = getattr(args, "page_id", "") or ""
    title_filter = getattr(args, "title", "") or ""
    for page in inventory.get("pages") or []:
        if page_limit is not None and counts["pages_selected"] >= page_limit:
            break
        if page.get("is_me") and not include_own:
            counts["pages_skip_own"] += 1
            continue
        if category_filter and category_filter.casefold() not in str(page.get("category_name") or "").casefold():
            continue
        if page_id_filter and page_id_filter != str(page.get("id")):
            continue
        if title_filter and title_filter != str(page.get("title")):
            continue
        counts["pages_selected"] += 1
        page_actions: list[dict[str, Any]] = []
        for remote_file in page.get("files") or []:
            counts["files_total"] += 1
            size = int(remote_file.get("size") or 0)
            md5 = str(remote_file.get("md5") or "").lower() or None
            already = False
            for local in local_by_size.get(size, []):
                if md5:
                    if md5 in local_md5_sets.get(size, set()):
                        already = True
                        break
                    try:
                        value = local_md5(local, hash_cache)
                    except Exception:
                        continue
                    local_md5_sets.setdefault(size, set()).add(value)
                    if value == md5:
                        already = True
                        break
                elif Path(local["filename"]).name.casefold() == str(remote_file.get("filename") or "").casefold():
                    already = True
                    break
            if already:
                counts["files_already_local"] += 1
                continue
            dest, same = unique_destination(
                destination_for(page, remote_file), reserved, md5
            )
            if same:
                counts["files_already_local"] += 1
                continue
            if max_files is not None and counts["files_ready"] >= max_files:
                skipped.append({"page_id": page.get("id"), "reason": "max_files"})
                break
            if max_bytes is not None and counts["bytes_ready"] + size > max_bytes:
                skipped.append({"page_id": page.get("id"), "reason": "max_mb"})
                break
            page_actions.append(
                {
                    "page_id": page.get("id"),
                    "page_title": page.get("title"),
                    "category_name": page.get("category_name"),
                    "category_parent": page.get("category_parent"),
                    "file_id": remote_file.get("id"),
                    "filename": remote_file.get("filename"),
                    "size": size,
                    "md5": md5,
                    "dest": str(dest),
                    "dest_rel": dest.relative_to(ROOT).as_posix(),
                }
            )
            counts["files_ready"] += 1
            counts["bytes_ready"] += size
        actions.extend(page_actions)
        if max_files is not None and counts["files_ready"] >= max_files:
            break
        if max_bytes is not None and counts["bytes_ready"] >= max_bytes:
            break
    return {"counts": counts, "actions": actions, "skipped": skipped}


def execute_download(plan: dict[str, Any], token: str, state: dict[str, Any]) -> dict[str, Any]:
    results = {"files_done": 0, "bytes_done": 0, "errors": []}
    lock = threading.Lock()

    def one(action: dict[str, Any]) -> dict[str, Any]:
        dest = Path(action["dest"])
        result = api.download_file(
            token,
            action["file_id"],
            dest,
            expected_md5=action.get("md5"),
            expected_size=action.get("size"),
        )
        with lock:
            state.setdefault("downloads", {})[action["file_id"]] = {
                "dest_rel": action["dest_rel"],
                "md5": result["md5"],
                "size": result["size"],
                "ts": int(time.time()),
            }
            api.atomic_write_json(SYNC_STATE, state)
        return result

    workers = max(1, int(plan.get("workers") or 4))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(one, action): action for action in plan.get("actions") or []}
        for future in as_completed(futures):
            action = futures[future]
            try:
                result = future.result()
                results["files_done"] += 1
                results["bytes_done"] += int(result.get("size") or 0)
                log(f"[download] ok: {action['dest_rel']}")
            except Exception as exc:
                with lock:
                    state.setdefault("errors", []).append(
                        {
                            "file_id": action.get("file_id"),
                            "dest_rel": action.get("dest_rel"),
                            "error": str(exc),
                            "ts": int(time.time()),
                        }
                    )
                    api.atomic_write_json(SYNC_STATE, state)
                results["errors"].append(
                    {"file_id": action.get("file_id"), "dest_rel": action.get("dest_rel"), "error": str(exc)}
                )
                log(f"[download] failed: {action.get('dest_rel')}: {exc}")
    return results


def refresh_index() -> dict[str, int]:
    sys.path.insert(0, str(REPO))
    from cooper_bot.modules.files.filesvc import FileService

    return FileService().build_find_index()


def print_summary(data: dict[str, Any], json_mode: bool) -> None:
    if json_mode:
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return
    print(json.dumps(data, ensure_ascii=False, indent=2))


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manual 1037wiki material-library sync")
    parser.add_argument("--token-file", default=str(REPO / api.DEFAULT_TOKEN_PATH))
    parser.add_argument("--json", action="store_true", help="print machine-readable JSON")
    sub = parser.add_subparsers(dest="command", required=True)

    inv = sub.add_parser("inventory", help="compare remote wiki with the local library")
    inv.add_argument("--refresh", action="store_true", help="refetch remote page details")
    inv.add_argument("--scope", choices=["indexed", "documents", "all"], default="indexed")
    inv.add_argument("--max-pages", type=int, default=None)

    up = sub.add_parser("upload", help="upload local-only material groups")
    up.add_argument("--execute", action="store_true")
    up.add_argument("--scope", choices=["indexed", "documents", "all"], default="indexed")
    up.add_argument("--subject", default="")
    up.add_argument("--title", default="")
    up.add_argument("--limit", type=int, default=None)
    up.add_argument("--include-copyright-risk", action="store_true")
    up.add_argument("--allow-partial", action="store_true")
    up.add_argument("--allow-existing-title", action="store_true")
    up.add_argument("--force", action="store_true", help="reprocess titles already uploaded by this sync tool")
    up.add_argument("--refresh", action="store_true")
    up.add_argument("--refresh-index", action="store_true")

    down = sub.add_parser("download", help="download remote-only files into the library")
    down.add_argument("--execute", action="store_true")
    down.add_argument("--include-own", action="store_true", help="include pages uploaded by this account")
    down.add_argument("--category", default="")
    down.add_argument("--page-id", default="")
    down.add_argument("--title", default="")
    down.add_argument("--limit", type=int, default=None, help="limit selected pages")
    down.add_argument("--max-files", type=int, default=None)
    down.add_argument("--max-mb", type=float, default=None)
    down.add_argument("--workers", type=int, default=4)
    down.add_argument("--refresh", action="store_true")
    down.add_argument("--refresh-index", action="store_true")

    sync = sub.add_parser("sync", help="inventory + upload plan + download plan")
    sync.add_argument("--execute", action="store_true")
    sync.add_argument("--scope", choices=["indexed", "documents", "all"], default="indexed")
    sync.add_argument("--refresh", action="store_true")
    sync.add_argument("--include-copyright-risk", action="store_true")
    sync.add_argument("--allow-partial", action="store_true")
    sync.add_argument("--allow-existing-title", action="store_true")
    sync.add_argument("--force", action="store_true")
    sync.add_argument("--include-own", action="store_true")
    sync.add_argument("--max-files", type=int, default=None)
    sync.add_argument("--max-mb", type=float, default=None)
    sync.add_argument("--limit", type=int, default=None)
    sync.add_argument("--refresh-index", action="store_true")

    for subparser in (inv, up, down, sync):
        subparser.add_argument("--json", action="store_true", help="print machine-readable JSON")

    ri = sub.add_parser("refresh-index", help="rebuild the bot /find index after sync")
    ri.add_argument("--json", action="store_true")
    tc = sub.add_parser("token-check", help="verify the current token")
    tc.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    token = api.load_token(args.token_file)
    if args.command == "token-check":
        data = api.api_request(token, "GET", "/page/list", params={"page": 1, "page_size": 1})
        print(json.dumps({"ok": True, "pages": len((data or {}).get("items") or [])}, ensure_ascii=False))
        return 0
    if args.command == "refresh-index":
        print_summary({"find_index": refresh_index()}, args.json)
        return 0

    inventory = api.build_remote_inventory(
        token,
        cache_path=REMOTE_CACHE,
        refresh=args.refresh,
        max_pages=getattr(args, "max_pages", None),
    )
    remote = remote_index(inventory)
    meta = load_index_meta()
    hash_cache = load_hash_cache()
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    if args.command == "inventory":
        rows = iter_local_files(args.scope, meta)
        local_sizes = {r["size"] for r in rows}
        local_only = 0
        local_md5_set: set[str] = set()
        for row in rows:
            if row["size"] not in remote["sizes"]:
                local_only += 1
                continue
            value = local_md5(row, hash_cache)
            local_md5_set.add(value)
            if value not in remote["md5"]:
                local_only += 1
        remote_only = 0
        remote_own = 0
        local_by_size: dict[int, list[dict[str, Any]]] = {}
        for row in rows:
            local_by_size.setdefault(row["size"], []).append(row)
        for f in remote["files"]:
            if f.get("is_me"):
                remote_own += 1
            size = int(f.get("size") or 0)
            md5 = str(f.get("md5") or "").lower()
            found = False
            for row in local_by_size.get(size, []):
                value = local_md5(row, hash_cache)
                if md5 and value == md5:
                    found = True
                    break
                if not md5 and row["filename"].casefold() == str(f.get("filename") or "").casefold():
                    found = True
                    break
            if not found:
                remote_only += 1
        save_hash_cache(hash_cache)
        report = {
            "remote_pages": inventory.get("page_count"),
            "remote_files": inventory.get("file_count"),
            "remote_own_files": remote_own,
            "local_files": len(rows),
            "remote_only_files": remote_only,
            "local_only_files": local_only,
            "scope": args.scope,
            "fetched_at": inventory.get("fetched_at"),
            "cache_errors": len(inventory.get("errors") or []),
        }
        api.atomic_write_json(REPORT, report)
        print_summary(report, args.json)
        return 0

    if args.command == "upload":
        pages = build_groups(iter_local_files(args.scope, meta))
        plan = plan_upload(pages, remote, hash_cache, args)
        plan = filter_available_categories(plan, token)
        save_hash_cache(hash_cache)
        api.atomic_write_json(SYNC_PLAN, {"kind": "upload", **plan})
        result: dict[str, Any] = {"action": "upload", "execute": bool(args.execute), **plan["counts"]}
        if args.execute:
            state = api.read_json(SYNC_STATE, {"pages": {}, "files": {}, "downloads": {}, "errors": []})
            result["executed"] = execute_upload(plan, token, state)
            if args.refresh_index:
                result["find_index"] = refresh_index()
        else:
            result["sample"] = [
                {"title": a["title"], "files": len(a["files"]), "bytes": sum(f["size"] for f in a["files"])}
                for a in plan["actions"][:20]
            ]
        print_summary(result, args.json)
        return 0

    if args.command == "download":
        plan = plan_download(inventory, remote, hash_cache, args)
        save_hash_cache(hash_cache)
        plan["workers"] = args.workers
        api.atomic_write_json(SYNC_PLAN, {"kind": "download", **plan})
        result: dict[str, Any] = {"action": "download", "execute": bool(args.execute), **plan["counts"]}
        if args.execute:
            state = api.read_json(SYNC_STATE, {"pages": {}, "files": {}, "downloads": {}, "errors": []})
            result["executed"] = execute_download(plan, token, state)
            if args.refresh_index:
                result["find_index"] = refresh_index()
        else:
            result["sample"] = plan["actions"][:20]
        print_summary(result, args.json)
        return 0

    if args.command == "sync":
        pages = build_groups(iter_local_files(args.scope, meta))
        upload_plan = plan_upload(pages, remote, hash_cache, args)
        upload_plan = filter_available_categories(upload_plan, token)
        download_plan = plan_download(inventory, remote, hash_cache, args)
        save_hash_cache(hash_cache)
        result = {
            "action": "sync",
            "execute": bool(args.execute),
            "remote_pages": inventory.get("page_count"),
            "remote_files": inventory.get("file_count"),
            "upload": upload_plan["counts"],
            "download": download_plan["counts"],
        }
        api.atomic_write_json(SYNC_PLAN, {"kind": "sync", "upload": upload_plan, "download": download_plan})
        if args.execute:
            state = api.read_json(SYNC_STATE, {"pages": {}, "files": {}, "downloads": {}, "errors": []})
            result["executed_upload"] = execute_upload(upload_plan, token, state)
            result["executed_download"] = execute_download(download_plan, token, state)
            if args.refresh_index:
                result["find_index"] = refresh_index()
        print_summary(result, args.json)
        return 0

    raise SystemExit(f"unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
