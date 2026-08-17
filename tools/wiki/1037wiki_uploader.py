import argparse
import hashlib
import json
import mimetypes
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
WIKI_DIR = Path(__file__).resolve().parent
WIKI_STATE_DIR = REPO_ROOT / "runtime" / "state" / "wiki"
PLAN_PATH = WIKI_STATE_DIR / "1037wiki_upload_plan.json"
TOKEN_PATH = REPO_ROOT / "config" / "private" / "url_and_token.txt"
STATE_PATH = WIKI_STATE_DIR / "1037wiki_upload_state.json"
API_BASE = "https://api.1037.wiki"
MAX_FILE_SIZE = 200 * 1024 * 1024

COPYRIGHT_RISK_MARKERS = (
    "Z-Library",
    "z-library",
    "\u6559\u6750",
    "\u8bfe\u672c",
    "\u7b2c\u516d\u7248",
    "\u7b2c\u4e03\u7248",
    "\u4e2d\u6587\u7248",
    "\u51fa\u7248\u793e",
)

SUBJECT_ALIASES = {
    "习思想_习概": ["习概"],
    "信号与系统": ["信号"],
    "马原": ["马克思主义基本原理"],
    "毛概": ["毛泽东思想和中国特色社会主义理论体系概论"],
    "数电": ["数字电子技术基础", "数字电路"],
    "模电": ["模拟电子技术"],
    "大学物理": ["大物"],
    "大物实验": ["大学物理实验"],
}


class ApiError(RuntimeError):
    pass


def load_url_and_token() -> tuple[str, str]:
    lines = TOKEN_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
    url_match = re.search(r"https?://\S+", lines[0] if lines else "")
    site_url = url_match.group(0).rstrip("\uff0c,。") if url_match else "https://1037.wiki/"
    raw_token = lines[1].strip() if len(lines) > 1 else ""
    token_match = re.search(r"eyJ[\w.-]+", raw_token)
    token = token_match.group(0) if token_match else raw_token
    if not token:
        raise ApiError(f"missing token in {TOKEN_PATH}")
    return site_url, token


def load_json(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    return json.loads(path.read_text(encoding="utf-8"))


def save_json_atomic(path: Path, data: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8", newline="\n")
    last_error: Exception | None = None
    for _ in range(5):
        try:
            tmp.replace(path)
            return
        except PermissionError as exc:
            last_error = exc
            time.sleep(0.2)
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8", newline="\n")
        tmp.unlink(missing_ok=True)
    except Exception:
        if last_error:
            raise last_error
        raise


def api_request(
    token: str,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
    timeout: float = 60.0,
) -> Any:
    url = API_BASE.rstrip("/") + "/" + path.lstrip("/")
    if params:
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        if query:
            url += "?" + query
    body = None
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
    }
    if data is not None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=body, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            text = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        raise ApiError(f"HTTP {exc.code} {path}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise ApiError(f"network error {path}: {exc}") from exc

    try:
        payload = json.loads(text) if text else None
    except json.JSONDecodeError as exc:
        raise ApiError(f"non-json response {path}: {text[:200]}") from exc

    if isinstance(payload, dict) and "code" in payload:
        if payload.get("code") not in (0, None):
            raise ApiError(f"api error {path}: code={payload.get('code')} message={payload.get('message')}")
        return payload.get("data")
    return payload


def put_file(upload_url: str, path: Path) -> None:
    content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    data = path.read_bytes()
    req = urllib.request.Request(
        upload_url,
        data=data,
        headers={"Content-Type": content_type, "User-Agent": "Mozilla/5.0"},
        method="PUT",
    )
    try:
        with urllib.request.urlopen(req, timeout=max(60, len(data) / 1024 / 1024 * 20)) as response:
            if response.status < 200 or response.status >= 300:
                raise ApiError(f"S3 upload failed: {response.status}")
    except urllib.error.HTTPError as exc:
        raise ApiError(f"S3 upload failed: HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise ApiError(f"S3 upload network error: {exc}") from exc


def file_md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def upload_file(token: str, path: Path) -> tuple[str, bool]:
    if path.stat().st_size > MAX_FILE_SIZE:
        raise ApiError("\u5355\u6587\u4ef6\u8d85\u8fc7 200MB\uff0c\u524d\u7aef\u4e5f\u4e0d\u5141\u8bb8\u4e0a\u4f20")
    md5 = file_md5(path)
    pre = api_request(token, "POST", "/file/upload/url", data={"file_name": path.name, "md5": md5}, timeout=60)
    if isinstance(pre, dict) and pre.get("file_id"):
        return str(pre["file_id"]), True
    if not isinstance(pre, dict) or not pre.get("upload_url") or not pre.get("file_name"):
        raise ApiError(f"unexpected upload-url response for {path.name}")
    put_file(str(pre["upload_url"]), path)
    file_id = api_request(token, "POST", "/file/upload/finish", data={"file_name": pre["file_name"]}, timeout=60)
    if not file_id:
        raise ApiError(f"missing file_id after upload finish for {path.name}")
    return str(file_id), False


def cached_file_id(value: Any) -> tuple[str | None, bool]:
    if isinstance(value, dict):
        file_id = value.get("id")
        if file_id:
            return str(file_id), bool(value.get("reused_existing"))
        return None, False
    if value:
        return str(value), True
    return None, False


def find_category(token: str, parent: str, name: str, aliases: list[str] | None = None) -> dict[str, Any] | None:
    candidates = {str(name or "").strip()}
    for alias in aliases or []:
        alias = str(alias or "").strip()
        if alias:
            candidates.add(alias)
    for part in re.split(r"[_\s/]+", str(name or "")):
        part = part.strip()
        if part:
            candidates.add(part)
    for page in range(1, 21):
        data = api_request(
            token,
            "GET",
            "/category/list",
            params={"parent": parent, "page": page, "page_size": 20},
        )
        items = data.get("items") if isinstance(data, dict) else []
        for item in items or []:
            if not isinstance(item, dict) or item.get("parent") != parent:
                continue
            item_aliases = {str(v).strip() for v in (item.get("aliases") or []) if str(v).strip()}
            item_names = {str(item.get("name") or "").strip()} | item_aliases
            if candidates & item_names:
                return item
        paging = data.get("paging") if isinstance(data, dict) else {}
        if not isinstance(paging, dict) or not paging.get("has_more"):
            break
    return None


def get_or_create_category(token: str, parent: str, name: str, aliases: list[str], execute: bool) -> str:
    aliases = list(aliases or []) + SUBJECT_ALIASES.get(name, [])
    found = find_category(token, parent, name, aliases)
    if found and found.get("id"):
        return str(found["id"])
    if not execute:
        return f"DRY_CATEGORY:{parent}:{name}"
    created = api_request(
        token,
        "POST",
        "/category",
        data={"name": name, "parent": parent, "aliases": aliases[:10]},
    )
    if not isinstance(created, dict) or not created.get("id"):
        raise ApiError(f"missing category id for {name}")
    return str(created["id"])


def page_has_copyright_risk(page: dict[str, Any]) -> bool:
    text = json.dumps(page, ensure_ascii=False)
    return any(marker in text for marker in COPYRIGHT_RISK_MARKERS)


def page_summary(page: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": page.get("title"),
        "file_count": page.get("file_count"),
        "size_mb": page.get("size_mb"),
        "upload_mode_hint": page.get("upload_mode_hint"),
    }


def run(args: argparse.Namespace) -> int:
    _site_url, token = load_url_and_token()
    plan = load_json(PLAN_PATH, {})
    pages: list[dict[str, Any]] = list(plan.get("pages") or [])
    state = load_json(STATE_PATH, {"pages": {}, "files": {}, "errors": []})
    execute = bool(args.execute)

    selected = pages
    if args.title:
        selected = [page for page in selected if str(page.get("title")) == args.title]
    if args.start:
        selected = selected[int(args.start) :]
    if args.limit is not None:
        selected = selected[: int(args.limit)]

    print(
        json.dumps(
            {
                "mode": "execute" if execute else "dry-run",
                "selected_pages": len(selected),
                "state": str(STATE_PATH),
                "skip_copyright_risk": not args.include_copyright_risk,
            },
            ensure_ascii=False,
        )
    )

    counts: dict[str, int] = {
        "page_done": 0,
        "skip_existing": 0,
        "skip_copyright_risk": 0,
        "skip_large_file": 0,
        "page_error": 0,
    }
    for idx, page in enumerate(selected, start=1):
        title = str(page.get("title") or "").strip()
        page_state = state["pages"].get(title)
        if execute and page_state and page_state.get("page_id") and not page_state.get("dry_run") and not args.force:
            counts["skip_existing"] += 1
            print(json.dumps({"event": "skip_existing", **page_summary(page)}, ensure_ascii=False))
            continue
        if page_has_copyright_risk(page) and not args.include_copyright_risk:
            counts["skip_copyright_risk"] += 1
            print(json.dumps({"event": "skip_copyright_risk", **page_summary(page)}, ensure_ascii=False))
            continue

        print(json.dumps({"event": "page_start", "index": idx, **page_summary(page)}, ensure_ascii=False))
        try:
            aliases = [tag for tag in page.get("tags", []) if tag not in {page.get("subject"), page.get("material_category")}]
            category_id = get_or_create_category(
                token,
                str(page.get("parent_category") or "\u8bfe\u7a0b"),
                str(page.get("subject") or page.get("title")),
                aliases,
                execute,
            )

            file_ids: list[str] = []
            file_refs: list[dict[str, Any]] = []
            for file_item in page.get("files") or []:
                path = Path(str(file_item.get("path") or ""))
                rel = str(file_item.get("rel") or path.name)
                cached_id, cached_reused = cached_file_id(state["files"].get(rel))
                if cached_id and not args.force:
                    file_ids.append(str(cached_id))
                    file_refs.append({"rel": rel, "file_id": str(cached_id), "reused_existing": cached_reused})
                    continue
                if not path.exists():
                    raise ApiError(f"missing file: {path}")
                if path.stat().st_size > MAX_FILE_SIZE:
                    counts["skip_large_file"] += 1
                    print(
                        json.dumps(
                            {
                                "event": "skip_large_file",
                                "rel": rel,
                                "size_mb": round(path.stat().st_size / 1024 / 1024, 2),
                            },
                            ensure_ascii=False,
                        )
                    )
                    continue
                if execute:
                    file_id, reused_existing = upload_file(token, path)
                    state["files"][rel] = {"id": file_id, "reused_existing": reused_existing}
                    save_json_atomic(STATE_PATH, state)
                else:
                    file_id = "DRY_FILE:" + rel
                    reused_existing = False
                file_ids.append(file_id)
                file_refs.append({"rel": rel, "file_id": file_id, "reused_existing": reused_existing})
                print(json.dumps({"event": "file_ready", "rel": rel, "execute": execute}, ensure_ascii=False))

            if not file_ids:
                raise ApiError("no uploadable files for page")

            if execute:
                page_payload = {
                    "title": title,
                    "tags": page.get("tags") or [],
                    "description": page.get("description") or title,
                    "category_id": category_id,
                    "collection_ids": None,
                    "anonymous": False,
                    "file_ids": file_ids,
                }
                try:
                    page_id = api_request(token, "POST", "/page", data=page_payload, timeout=60)
                except ApiError as exc:
                    if "403" not in str(exc) or "\u65e0\u6743\u9650\u4f7f\u7528\u8be5\u6587\u4ef6" not in str(exc):
                        raise
                    owned_file_ids = [
                        ref["file_id"]
                        for ref in file_refs
                        if ref.get("file_id")
                        and (not ref.get("reused_existing") or str(ref.get("file_id")).startswith("6a"))
                    ]
                    skipped_reused = [
                        ref["rel"]
                        for ref in file_refs
                        if ref.get("reused_existing") and not str(ref.get("file_id")).startswith("6a")
                    ]
                    if not owned_file_ids:
                        raise
                    print(
                        json.dumps(
                            {
                                "event": "retry_without_reused_files",
                                "title": title,
                                "skipped_files": skipped_reused,
                            },
                            ensure_ascii=False,
                        )
                    )
                    page_payload["file_ids"] = owned_file_ids
                    page_id = api_request(token, "POST", "/page", data=page_payload, timeout=60)
                    file_ids = owned_file_ids
            else:
                page_id = "DRY_PAGE:" + title
            if execute:
                state["pages"][title] = {
                    "page_id": page_id,
                    "category_id": category_id,
                    "file_ids": file_ids,
                    "updated_ts": int(time.time()),
                    "dry_run": False,
                }
                save_json_atomic(STATE_PATH, state)
            counts["page_done"] += 1
            print(json.dumps({"event": "page_done", "title": title, "page_id": page_id, "execute": execute}, ensure_ascii=False))
        except Exception as exc:
            counts["page_error"] += 1
            err = {"title": title, "error": str(exc), "ts": int(time.time())}
            if execute:
                state.setdefault("errors", []).append(err)
                save_json_atomic(STATE_PATH, state)
            print(json.dumps({"event": "page_error", **err}, ensure_ascii=False), file=sys.stderr)
            if args.stop_on_error:
                return 1

    print(json.dumps({"event": "summary", **counts, "execute": execute}, ensure_ascii=False))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload planned materials to 1037wiki.")
    parser.add_argument("--execute", action="store_true", help="Actually create categories, upload files, and create pages.")
    parser.add_argument("--include-copyright-risk", action="store_true", help="Include pages that look like published textbooks/books.")
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N selected pages.")
    parser.add_argument("--start", type=int, default=0, help="Skip the first N selected pages.")
    parser.add_argument("--title", default="", help="Only process an exact page title.")
    parser.add_argument("--force", action="store_true", help="Ignore existing state and reprocess selected pages.")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop at the first failed page.")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
