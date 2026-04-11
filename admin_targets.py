from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import config


_SPACES_RE = re.compile(r"\s+")


@dataclass
class TargetResolveResult:
    ok: bool
    target_id: Optional[int] = None
    status: str = ""
    query: str = ""
    message: str = ""
    candidates: list[int] = field(default_factory=list)


def _canon(text: Any) -> str:
    raw = unicodedata.normalize("NFKC", str(text or "")).strip().casefold()
    return _SPACES_RE.sub("", raw)


def _normalize_int_id(value: Any) -> Optional[int]:
    try:
        n = int(value)
    except Exception:
        return None
    if n <= 0:
        return None
    return n


class AdminTargetResolver:
    def __init__(self) -> None:
        self._loaded_path: str = ""
        self._loaded_mtime_ns: int = -1
        self._group_alias_ids: dict[str, set[int]] = {}
        self._user_alias_ids: dict[str, set[int]] = {}
        self._last_warning_key: str = ""

    @staticmethod
    def _config_path() -> Path:
        data_dir = getattr(config, "DATA_DIR", Path("data"))
        return Path(data_dir) / "admin_targets.json"

    def _warn_once(self, key: str, message: str, logsvc: Any = None) -> None:
        if key == self._last_warning_key:
            return
        self._last_warning_key = key
        try:
            if logsvc is not None and hasattr(logsvc, "log") and hasattr(logsvc.log, "warning"):
                logsvc.log.warning(message)
        except Exception:
            pass

    @staticmethod
    def _build_alias_ids(raw_map: Any) -> dict[str, set[int]]:
        out: dict[str, set[int]] = {}
        if not isinstance(raw_map, dict):
            return out
        for alias, target in raw_map.items():
            key = _canon(alias)
            if not key:
                continue
            tid = _normalize_int_id(target)
            if tid is None:
                continue
            bucket = out.setdefault(key, set())
            bucket.add(tid)
        return out

    def _reload_if_needed(self, logsvc: Any = None) -> None:
        path = self._config_path()
        path_str = str(path.resolve() if path.exists() else path)
        exists = path.exists()
        mtime_ns = -1
        if exists:
            try:
                st = path.stat()
                mtime_ns = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000)))
            except Exception:
                mtime_ns = -1

        if (path_str == self._loaded_path) and (mtime_ns == self._loaded_mtime_ns):
            return

        self._loaded_path = path_str
        self._loaded_mtime_ns = mtime_ns
        self._group_alias_ids = {}
        self._user_alias_ids = {}

        if not exists:
            return

        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            self._warn_once(
                f"parse:{path_str}:{e}",
                f"admin targets config parse failed: {path} err={e}",
                logsvc=logsvc,
            )
            return
        if not isinstance(obj, dict):
            self._warn_once(
                f"type:{path_str}",
                f"admin targets config invalid type: {path}",
                logsvc=logsvc,
            )
            return

        self._group_alias_ids = self._build_alias_ids(obj.get("groups"))
        self._user_alias_ids = self._build_alias_ids(obj.get("users"))

    @staticmethod
    def _extract_explicit_group_id(token: str) -> Optional[int]:
        t = unicodedata.normalize("NFKC", str(token or "")).strip()
        candidates = [t]
        if t.startswith("群"):
            candidates.append(t[1:].strip())
        if t.startswith("QQ群"):
            candidates.append(t[2:].strip())
        if t.endswith("群"):
            candidates.append(t[:-1].strip())
        for one in candidates:
            if one.isdigit():
                return _normalize_int_id(one)
        return None

    @staticmethod
    def _extract_explicit_user_id(token: str) -> Optional[int]:
        t = unicodedata.normalize("NFKC", str(token or "")).strip()
        candidates = [t]
        if t.startswith("QQ"):
            candidates.append(t[2:].strip())
        if t.startswith("用户"):
            candidates.append(t[2:].strip())
        for one in candidates:
            if one.isdigit():
                return _normalize_int_id(one)
        return None

    @staticmethod
    def _alias_query_variants(token: str, *, target_type: str) -> list[str]:
        t = unicodedata.normalize("NFKC", str(token or "")).strip()
        out = [t]
        if target_type == "group":
            if t.startswith("群"):
                out.append(t[1:].strip())
            if t.startswith("QQ群"):
                out.append(t[2:].strip())
            if t.endswith("群"):
                out.append(t[:-1].strip())
            out.append(f"{t}群")
        elif target_type == "user":
            if t.startswith("QQ"):
                out.append(t[2:].strip())
            if t.startswith("用户"):
                out.append(t[2:].strip())
        dedup: list[str] = []
        seen = set()
        for one in out:
            key = _canon(one)
            if (not key) or (key in seen):
                continue
            seen.add(key)
            dedup.append(one)
        return dedup

    def _resolve_with_alias_map(
        self,
        token: str,
        *,
        target_type: str,
        alias_ids: dict[str, set[int]],
    ) -> TargetResolveResult:
        raw = unicodedata.normalize("NFKC", str(token or "")).strip()
        if not raw:
            return TargetResolveResult(ok=False, status="invalid", query=raw, message="目标不能为空。")

        if target_type == "group":
            explicit = self._extract_explicit_group_id(raw)
        else:
            explicit = self._extract_explicit_user_id(raw)
        if explicit is not None:
            return TargetResolveResult(ok=True, target_id=explicit, status="id", query=raw)

        hit_ids: set[int] = set()
        for one in self._alias_query_variants(raw, target_type=target_type):
            ids = alias_ids.get(_canon(one), set())
            hit_ids.update(int(x) for x in ids)

        if not hit_ids:
            return TargetResolveResult(
                ok=False,
                status="not_found",
                query=raw,
                message=f"未找到{target_type}别名：{raw}",
            )
        if len(hit_ids) > 1:
            return TargetResolveResult(
                ok=False,
                status="ambiguous",
                query=raw,
                message=f"{target_type}别名存在多个候选：{raw}",
                candidates=sorted(hit_ids),
            )
        return TargetResolveResult(ok=True, target_id=next(iter(hit_ids)), status="alias", query=raw)

    def resolve_group_target(self, token: str, logsvc: Any = None) -> TargetResolveResult:
        if not bool(getattr(config, "ENABLE_ADMIN_TARGET_ALIASES", True)):
            return self._resolve_with_alias_map(
                token,
                target_type="group",
                alias_ids={},
            )
        self._reload_if_needed(logsvc=logsvc)
        return self._resolve_with_alias_map(
            token,
            target_type="group",
            alias_ids=self._group_alias_ids,
        )

    def resolve_user_target(self, token: str, logsvc: Any = None) -> TargetResolveResult:
        if not bool(getattr(config, "ENABLE_ADMIN_TARGET_ALIASES", True)):
            return self._resolve_with_alias_map(
                token,
                target_type="user",
                alias_ids={},
            )
        self._reload_if_needed(logsvc=logsvc)
        return self._resolve_with_alias_map(
            token,
            target_type="user",
            alias_ids=self._user_alias_ids,
        )


_resolver = AdminTargetResolver()


def resolve_group_target(token: str, logsvc: Any = None) -> TargetResolveResult:
    return _resolver.resolve_group_target(token, logsvc=logsvc)


def resolve_user_target(token: str, logsvc: Any = None) -> TargetResolveResult:
    return _resolver.resolve_user_target(token, logsvc=logsvc)


def clear_target_resolver_cache() -> None:
    _resolver._loaded_path = ""
    _resolver._loaded_mtime_ns = -1
    _resolver._group_alias_ids = {}
    _resolver._user_alias_ids = {}
