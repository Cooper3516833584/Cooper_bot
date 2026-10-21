"""Small, dependency-free client for the 1037wiki HTTP API.

This module is intentionally independent from the bot runtime.  It is shared by
the manual sync CLI and may be imported by other agent-side tools.
"""
from __future__ import annotations

import hashlib
import gzip
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable

API_BASE = "https://api.1037.wiki"
SITE_URL = "https://1037.wiki/"
DEFAULT_TOKEN_PATH = Path("config/private/url_and_token.txt")
DEFAULT_STATE_DIR = Path("runtime/state/wiki")
REMOTE_CACHE_NAME = "1037wiki_remote_inventory.json"
MAX_UPLOAD_BYTES = 200 * 1024 * 1024
WAF_COOKIE_JS = Path(__file__).resolve().parent / "waf_cookie.js"

_waf_cookie: str | None = None
_waf_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_request_at = 0.0
MIN_REQUEST_INTERVAL = 0.5
_active_token: str | None = None
_token_path: Path = DEFAULT_TOKEN_PATH
_refresh_lock = threading.Lock()


def _wait_for_slot() -> None:
    global _next_request_at
    while True:
        with _rate_lock:
            now = time.monotonic()
            if now >= _next_request_at:
                _next_request_at = now + MIN_REQUEST_INTERVAL
                return
            wait = _next_request_at - now
        time.sleep(min(wait, 5.0))


def _defer(seconds: float) -> None:
    global _next_request_at
    with _rate_lock:
        _next_request_at = max(_next_request_at, time.monotonic() + max(0.0, seconds))


def solve_waf_cookie(html: str) -> str | None:
    """Execute the site's WAF challenge in a minimal Node vm and return its cookie."""
    global _waf_cookie
    match = re.search(r"<script>(.*?)</script>", html or "", re.S)
    if not match:
        return None
    script = match.group(1)
    with _waf_lock:
        handle = tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8")
        try:
            handle.write(script)
            handle.close()
            result = subprocess.run(
                ["node", str(WAF_COOKIE_JS), handle.name],
                capture_output=True,
                text=True,
                timeout=20,
            )
        except (FileNotFoundError, subprocess.SubprocessError):
            return None
        finally:
            try:
                os.unlink(handle.name)
            except OSError:
                pass
        cookie = (result.stdout or "").strip()
        if cookie and "acw_sc__v2=" in cookie:
            _waf_cookie = cookie.split(";", 1)[0].strip()
            return _waf_cookie
        return None


class WikiApiError(RuntimeError):
    pass


def load_token(token_path: str | Path | None = None) -> str:
    global _active_token, _token_path
    path = Path(token_path) if token_path else DEFAULT_TOKEN_PATH
    if not path.is_absolute():
        path = Path.cwd() / path
    _token_path = path
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    raw = lines[1].strip() if len(lines) > 1 else ""
    match = re.search(r"eyJ[\w.-]+", raw)
    token = match.group(0) if match else raw
    if not token:
        raise WikiApiError(f"missing token in {path}")
    _active_token = token
    return token


def refresh_access_token(failed_token: str | None = None) -> str | None:
    """Use the private refresh token to obtain a fresh access token."""
    global _active_token
    with _refresh_lock:
        if failed_token and _active_token and _active_token != failed_token:
            return _active_token
        refresh_path = _token_path.parent / "1037wiki_refresh_token.txt"
        if not refresh_path.exists():
            return None
        refresh_token = refresh_path.read_text(encoding="utf-8", errors="replace").strip()
        if not refresh_token:
            return None
        body = json.dumps({"refresh_token": refresh_token}).encode("utf-8")
        req = urllib.request.Request(
            API_BASE.rstrip("/") + "/auth/refresh",
            data=body,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8", errors="replace"))
        except Exception:
            return None
        data = (payload or {}).get("data") or {}
        access = str(data.get("access_token") or "")
        new_refresh = str(data.get("refresh_token") or "")
        if not access:
            return None
        _active_token = access
        try:
            _token_path.write_text("https://1037.wiki/\n" + access + "\n", encoding="utf-8")
            if new_refresh:
                refresh_path.write_text(new_refresh + "\n", encoding="utf-8")
        except OSError:
            pass
        return access


def token_payload(token: str) -> dict[str, Any]:
    import base64

    try:
        part = token.split(".")[1]
        part += "=" * (-len(part) % 4)
        return json.loads(base64.urlsafe_b64decode(part))
    except Exception:
        return {}


def api_request(
    token: str,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
    timeout: float = 60.0,
) -> Any:
    global _waf_cookie
    token = _active_token or token
    url = API_BASE.rstrip("/") + "/" + path.lstrip("/")
    if params:
        query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        if query:
            url += "?" + query
    body = None
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Referer": SITE_URL,
        "Origin": SITE_URL.rstrip("/"),
    }
    if data is not None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    idempotent = method.upper() in {"GET", "HEAD"} or path in {
        "/file/upload/url",
        "/file/upload/finish",
    }
    attempts = 5 if idempotent else 2
    last_error: Exception | None = None
    for attempt in range(attempts):
        headers["Authorization"] = f"Bearer {token}"
        with _waf_lock:
            cookie = _waf_cookie
        if cookie:
            headers["Cookie"] = cookie
        _wait_for_slot()
        req = urllib.request.Request(url, data=body, headers=headers, method=method.upper())
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                raw = response.read()
                encoding = str(response.headers.get("Content-Encoding") or "").lower()
                remaining = str(response.headers.get("x-ratelimit-remaining") or "")
                retry_after = str(response.headers.get("retry-after") or "")
                if remaining == "0":
                    try:
                        _defer(float(retry_after or 10))
                    except ValueError:
                        _defer(10)
                for cookie_header in response.headers.get_all("Set-Cookie") or []:
                    if "acw_sc__v2=" in cookie_header:
                        with _waf_lock:
                            _waf_cookie = cookie_header.split(";", 1)[0].strip()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
            last_error = WikiApiError(f"HTTP {exc.code} {path}: {detail}")
            if exc.code == 401:
                new_token = refresh_access_token(token)
                if new_token and attempt < attempts - 1:
                    token = new_token
                    print(
                        f"[1037wiki] access token refreshed, retrying {path}",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue
                raise last_error from exc
            if exc.code == 429:
                try:
                    _defer(float(exc.headers.get("retry-after") or 10))
                except ValueError:
                    _defer(10)
                if idempotent and attempt < attempts - 1:
                    print(
                        f"[1037wiki] HTTP 429 {path}, waiting for server quota",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue
            if not idempotent or exc.code not in {429, 500, 502, 503, 504} or attempt == attempts - 1:
                raise last_error from exc
        except urllib.error.URLError as exc:
            last_error = WikiApiError(f"network error {path}: {exc}")
            if not idempotent or attempt == attempts - 1:
                raise last_error from exc
        else:
            if encoding == "gzip" or raw[:2] == b"\x1f\x8b":
                try:
                    raw = gzip.decompress(raw)
                except OSError:
                    pass
            elif encoding == "deflate":
                try:
                    raw = zlib.decompress(raw)
                except zlib.error:
                    pass
            text = raw.decode("utf-8", errors="replace")
            if text.lstrip().startswith("<"):
                last_error = WikiApiError(f"anti-bot challenge on {path}")
                if idempotent:
                    solved = solve_waf_cookie(text)
                    if solved:
                        if attempt < attempts - 1:
                            time.sleep(1.0)
                            continue
                if not idempotent or attempt == attempts - 1:
                    raise last_error
            else:
                try:
                    payload = json.loads(text) if text else None
                except json.JSONDecodeError as exc:
                    last_error = WikiApiError(f"non-json response {path}: {text[:200]}")
                    if not idempotent or attempt == attempts - 1:
                        raise last_error from exc
                else:
                    if isinstance(payload, dict) and "code" in payload:
                        code = payload.get("code")
                        if code not in (0, None):
                            last_error = WikiApiError(
                                f"api error {path}: code={code} message={payload.get('message')}"
                            )
                            if idempotent and code in {429, 500, 502, 503, 504} and attempt < attempts - 1:
                                delay = min(30.0, 3.0 * (attempt + 1))
                                print(
                                    f"[1037wiki] rate limit {path}, retry in {delay:.0f}s",
                                    file=sys.stderr,
                                    flush=True,
                                )
                                _defer(delay)
                                continue
                            raise last_error
                        return payload.get("data")
                    return payload
        if attempt < attempts - 1:
            delay = min(60.0, 5.0 * (2 ** attempt))
            print(
                f"[1037wiki] retry {attempt + 1}/{attempts - 1} for {path} in {delay:.0f}s",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)
    if last_error:
        raise last_error
    raise WikiApiError(f"request failed: {path}")


def list_pages(token: str, *, page_size: int = 20, max_pages: int | None = None, request_delay: float = 0.3) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    page = 1
    while True:
        data = api_request(
            token,
            "GET",
            "/page/list",
            params={"page": page, "page_size": page_size},
        )
        batch = (data or {}).get("items") or []
        items.extend(item for item in batch if isinstance(item, dict))
        paging = (data or {}).get("paging") or {}
        if not paging.get("has_more"):
            break
        page += 1
        if max_pages is not None and page > max_pages:
            break
        if request_delay:
            time.sleep(request_delay)
    return items


def get_page(token: str, page_id: str) -> dict[str, Any]:
    data = api_request(token, "GET", f"/page/{page_id}")
    if not isinstance(data, dict) or not isinstance(data.get("page"), dict):
        raise WikiApiError(f"unexpected page response for {page_id}")
    page = dict(data["page"])
    page["is_me"] = bool(data.get("is_me"))
    return page


def get_download_url(token: str, file_id: str) -> str:
    url = api_request(token, "GET", f"/file/{file_id}/download")
    if not isinstance(url, str) or not url.startswith("http"):
        raise WikiApiError(f"missing download url for {file_id}")
    return url


def md5_file(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_file(
    token: str,
    file_id: str,
    destination: Path,
    *,
    expected_md5: str | None = None,
    expected_size: int | None = None,
    timeout: float = 120.0,
) -> dict[str, Any]:
    url = get_download_url(token, file_id)
    destination.parent.mkdir(parents=True, exist_ok=True)
    part = destination.with_name(destination.name + ".part")
    digest = hashlib.md5()
    size = 0
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": SITE_URL,
        "Accept": "*/*",
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response, part.open("wb") as f:
            for chunk in iter(lambda: response.read(1024 * 1024), b""):
                f.write(chunk)
                digest.update(chunk)
                size += len(chunk)
        actual_md5 = digest.hexdigest()
        if expected_size is not None and size != expected_size:
            raise WikiApiError(
                f"size mismatch for {file_id}: expected {expected_size}, got {size}"
            )
        if expected_md5 and actual_md5 != expected_md5:
            raise WikiApiError(
                f"md5 mismatch for {file_id}: expected {expected_md5}, got {actual_md5}"
            )
        os.replace(part, destination)
        return {
            "file_id": file_id,
            "path": str(destination),
            "size": size,
            "md5": actual_md5,
            "verified": True,
        }
    except Exception:
        part.unlink(missing_ok=True)
        raise


def sanitize_component(value: object, *, max_len: int = 90) -> str:
    text = str(value or "").strip()
    text = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", text)
    text = re.sub(r"\s+", " ", text).strip(" .")
    if text in {"", ".", ".."}:
        text = "_"
    reserved = {
        "CON", "PRN", "AUX", "NUL",
        *(f"COM{i}" for i in range(1, 10)),
        *(f"LPT{i}" for i in range(1, 10)),
    }
    if text.upper() in reserved:
        text = "_" + text
    return text[:max_len].rstrip(" .")


def atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
    os.replace(tmp, path)


def read_json(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


def build_remote_inventory(
    token: str,
    *,
    cache_path: Path | None = None,
    refresh: bool = False,
    page_workers: int = 2,
    max_pages: int | None = None,
    request_delay: float = 0.6,
    own_names: Iterable[str] = ("Cooper_Chen", "cooper_bot"),
) -> dict[str, Any]:
    cache = cache_path or (DEFAULT_STATE_DIR / REMOTE_CACHE_NAME)
    if not refresh and cache.exists():
        cached = read_json(cache, {})
        if isinstance(cached, dict) and cached.get("api_base") == API_BASE and cached.get("pages"):
            return cached

    payload = token_payload(token)
    own = {str(x).casefold() for x in own_names if str(x).strip()}
    if payload.get("nickname"):
        own.add(str(payload["nickname"]).casefold())

    pages = list_pages(token, page_size=20, max_pages=max_pages)
    records: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    def fetch(item: dict[str, Any]) -> dict[str, Any] | None:
        page_id = str(item.get("id") or "")
        if not page_id:
            return None
        if request_delay:
            time.sleep(request_delay)
        try:
            page = get_page(token, page_id)
        except Exception as exc:
            return {"__error__": {"page_id": page_id, "title": item.get("title"), "error": str(exc)}}
        creator = page.get("creator") or item.get("creator") or {}
        category = item.get("category") or {}
        files = []
        for f in page.get("files") or []:
            if not isinstance(f, dict):
                continue
            files.append(
                {
                    "id": str(f.get("id") or ""),
                    "filename": str(f.get("filename") or ""),
                    "size": int(f.get("size") or 0),
                    "md5": str(f.get("etag") or "").lower() or None,
                    "content_type": str(f.get("content_type") or ""),
                    "page_count": f.get("page_count"),
                    "downloads": f.get("downloads"),
                }
            )
        creator_name = str(creator.get("name") or "")
        is_me = bool(page.get("is_me")) or creator_name.casefold() in own
        return {
            "id": page_id,
            "title": str(page.get("title") or item.get("title") or ""),
            "description": str(page.get("description") or item.get("description") or ""),
            "tags": page.get("tags") or item.get("tags") or [],
            "category_id": str(page.get("category_id") or item.get("category_id") or ""),
            "category_name": str(category.get("name") or ""),
            "category_parent": str(category.get("parent") or ""),
            "creator_id": str(creator.get("id") or ""),
            "creator_name": creator_name,
            "is_me": is_me,
            "created_at": page.get("created_at") or item.get("created_at"),
            "updated_at": page.get("updated_at") or item.get("updated_at"),
            "files": files,
        }

    with ThreadPoolExecutor(max_workers=max(1, page_workers)) as pool:
        futures = [pool.submit(fetch, item) for item in pages]
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as exc:
                errors.append({"error": str(exc)})
                continue
            if not result:
                continue
            if "__error__" in result:
                errors.append(result["__error__"])
            else:
                records.append(result)

    records.sort(key=lambda p: (p.get("category_parent") or "", p.get("category_name") or "", p.get("title") or ""))
    inventory = {
        "api_base": API_BASE,
        "fetched_at": int(time.time()),
        "account": {
            "nickname": payload.get("nickname"),
            "sub": payload.get("sub"),
        },
        "page_count": len(records),
        "file_count": sum(len(p.get("files") or []) for p in records),
        "pages": records,
        "errors": errors,
    }
    atomic_write_json(cache, inventory)
    return inventory
