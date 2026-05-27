# filesvc.py
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional, Set, Tuple
import os
import re
import json
import time
import threading
import unicodedata

from config import (
    DOC_ROOTS,
    GROUP_DOCS_DIR,
    USER_DOCS_DIR,
    HANDIN_ROOT_DIR,
    UPLOAD_GROUP_HOST_DIR,
    UPLOAD_PRIVATE_HOST_DIR,
    LS_LIMIT,
    FIND_DIR_LIMIT,
    FIND_FILE_LIMIT,
    FIND_MAX_SCAN,
    DATA_DIR,
    DATA_DIR_CONTAINER,
)


_FIND_SPLIT_RE = re.compile(r"[\s\-_./\\,，;；:：|+&（）()【】\[\]{}<>《》“”\"'`~!@#$%^*=?]+")
_FIND_KEEP_RE = re.compile(r"[^0-9a-z\u4e00-\u9fff]+")
_FIND_BLOCKED_FILE_SUFFIXES = {
    ".aux",
    ".a",
    ".adoc",
    ".bak",
    ".bcf",
    ".bbl",
    ".blg",
    ".bmp",
    ".bat",
    ".bin",
    ".c",
    ".cat",
    ".cc",
    ".class",
    ".cmd",
    ".com",
    ".cpp",
    ".conf",
    ".cproj",
    ".cxx",
    ".css",
    ".download",
    ".dll",
    ".drv",
    ".elf",
    ".eps",
    ".exe",
    ".fdb_latexmk",
    ".fls",
    ".gif",
    ".gz",
    ".h",
    ".hex",
    ".heic",
    ".hh",
    ".hpp",
    ".htm",
    ".html",
    ".ico",
    ".img",
    ".inf",
    ".ini",
    ".ino",
    ".jar",
    ".java",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".lib",
    ".lds",
    ".log",
    ".lst",
    ".mbn",
    ".mui",
    ".nav",
    ".o",
    ".obj",
    ".old",
    ".only",
    ".otf",
    ".orig",
    ".out",
    ".part",
    ".pdb",
    ".pf",
    ".png",
    ".properties",
    ".py",
    ".pyc",
    ".pyo",
    ".rc",
    ".res",
    ".run.xml",
    ".sh",
    ".skip",
    ".snm",
    ".so",
    ".svg",
    ".swo",
    ".swp",
    ".synctex",
    ".sys",
    ".temp",
    ".tex",
    ".tif",
    ".tiff",
    ".tmp",
    ".toc",
    ".template",
    ".ttf",
    ".vrb",
    ".webp",
    ".xml",
    ".x",
    ".xbn",
    ".xn",
    ".xr",
    ".xu",
    ".yaml",
    ".yml",
}
_FIND_BLOCKED_FILE_NAMES = {".ds_store", "desktop.ini", "thumbs.db"}
_FIND_SKIP_DIR_NAMES = {".git", ".idea", "__pycache__"}
_FIND_LOW_SIGNAL_DIR_NAMES = {
    "assets",
    "back_cover",
    "fig",
    "figs",
    "image",
    "images",
    "titlepage",
    "video",
    "videos",
}

# Optional built-in alias groups for common course abbreviations.
_FIND_ALIAS_GROUPS = [
    {"模拟电子技术", "模拟电子", "模拟电路", "模电", "模电技术", "电子技术模拟", "电子技术模拟部分"},
    {"数字电子技术", "数字电子", "数字电路", "数字逻辑", "数电", "数电技术"},
    {"信号与系统", "信号系统", "信号"},
    {"大学物理", "大物"},
    {"大学物理实验", "大物实验"},
    {"习近平新时代中国特色社会主义思想概论", "习概", "习思想", "习思想概论"},
    {"毛泽东思想和中国特色社会主义理论体系概论", "毛概"},
    {"马克思主义基本原理", "马原"},
    {"中国近现代史纲要", "近现代史", "近代史"},
]


def _find_norm(text: str) -> str:
    s = unicodedata.normalize("NFKC", str(text or "")).casefold()
    s = _FIND_KEEP_RE.sub("", s)
    return s


def _dedupe_keep_order(items: List[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for x in items:
        if not x or x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _build_alias_map() -> Dict[str, List[str]]:
    table: Dict[str, Set[str]] = {}
    for group in _FIND_ALIAS_GROUPS:
        norm_group = sorted({_find_norm(x) for x in group if _find_norm(x)})
        if not norm_group:
            continue
        for term in norm_group:
            slot = table.setdefault(term, set())
            slot.update(norm_group)
    return {k: sorted(v) for k, v in table.items()}


_FIND_ALIAS_MAP = _build_alias_map()

@dataclass
class Root:
    name: str
    path: Path
    min_level: int

class FileService:
    _FIND_INDEX_FILENAME = "find_index.json"

    def __init__(self, log=None):
        self.log = log
        self.roots: List[Root] = [Root(n, Path(p), int(lv)) for (n, p, lv) in DOC_ROOTS]
        self._find_index_path = Path(DATA_DIR) / self._FIND_INDEX_FILENAME
        self._find_index_lock = threading.RLock()
        self._find_index_entries: List[dict] = []
        self._find_index_mtime_ns: int = 0
        self._find_index_loaded: bool = False

    def ensure_dirs(self):
        # 只保证“配置里定义的根”存在
        for r in self.roots:
            r.path.mkdir(parents=True, exist_ok=True)
        GROUP_DOCS_DIR.mkdir(parents=True, exist_ok=True)
        USER_DOCS_DIR.mkdir(parents=True, exist_ok=True)
        # Handin 根目录不对外暴露，但需要确保存在
        HANDIN_ROOT_DIR.mkdir(parents=True, exist_ok=True)
        # NapCat 专用上传目录（用于 /get 发送文件时 staging）
        UPLOAD_GROUP_HOST_DIR.mkdir(parents=True, exist_ok=True)
        UPLOAD_PRIVATE_HOST_DIR.mkdir(parents=True, exist_ok=True)

    def _log_info(self, msg: str) -> None:
        lg = self.log
        if lg is not None:
            try:
                lg.info(str(msg))
            except Exception:
                pass

    def _log_warning(self, msg: str) -> None:
        lg = self.log
        if lg is not None:
            try:
                lg.warning(str(msg))
            except Exception:
                pass

    @staticmethod
    def _path_under(base: Path, target: Path) -> bool:
        try:
            target.resolve().relative_to(base.resolve())
            return True
        except (OSError, RuntimeError, ValueError):
            return False

    @staticmethod
    def _norm_abs_path_str(p: object) -> str:
        try:
            s = os.path.abspath(str(p))
        except Exception:
            s = str(p or "")
        return os.path.normcase(s)

    @staticmethod
    def _path_under_norm(base_norm: str, target_norm: str) -> bool:
        b = str(base_norm or "")
        t = str(target_norm or "")
        if (not b) or (not t):
            return False
        if t == b:
            return True
        if b.endswith(os.sep):
            return t.startswith(b)
        return t.startswith(b + os.sep)

    @staticmethod
    def _find_dir_is_walkable(name: str) -> bool:
        return str(name or "").strip().casefold() not in _FIND_SKIP_DIR_NAMES

    @staticmethod
    def _find_dir_is_searchable(name: str) -> bool:
        return str(name or "").strip().casefold() not in _FIND_LOW_SIGNAL_DIR_NAMES

    @staticmethod
    def _find_file_is_searchable(path: Path) -> bool:
        name = str(path.name or "").strip().casefold()
        if (not name) or name.startswith("~$") or name in _FIND_BLOCKED_FILE_NAMES:
            return False
        suffixes = [str(s or "").casefold() for s in path.suffixes]
        if suffixes and "".join(suffixes[-2:]) in _FIND_BLOCKED_FILE_SUFFIXES:
            return False
        suffix = suffixes[-1] if suffixes else ""
        if not suffix:
            return False
        if suffix[1:].isdigit():
            return False
        return suffix not in _FIND_BLOCKED_FILE_SUFFIXES

    def _iter_find_index_bases(self) -> List[Tuple[str, Path]]:
        out: List[Tuple[str, Path]] = []
        seen = set()
        for r in self.roots:
            try:
                p = r.path.resolve()
            except Exception:
                p = r.path
            key = (str(r.name), os.path.normcase(str(p)))
            if key in seen:
                continue
            seen.add(key)
            out.append((str(r.name), p))
        try:
            g = GROUP_DOCS_DIR.resolve()
        except Exception:
            g = GROUP_DOCS_DIR
        g_key = ("groups", os.path.normcase(str(g)))
        if g_key not in seen:
            out.append(("groups", g))
        return out

    def _safe_write_json_atomic(self, path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        txt = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        tmp.write_text(txt, encoding="utf-8")
        os.replace(str(tmp), str(path))

    def _normalize_find_index_entry(self, raw: object) -> Optional[dict]:
        if not isinstance(raw, dict):
            return None
        p = str(raw.get("path") or "").strip()
        root_name = str(raw.get("root_name") or "").strip()
        root_path = str(raw.get("root_path") or "").strip()
        rel = str(raw.get("rel") or "").strip().replace("\\", "/")
        name = str(raw.get("name") or "").strip()
        if (not p) or (not root_name) or (not root_path) or (not rel) or (not name):
            return None
        is_dir = bool(raw.get("is_dir"))
        name_norm = str(raw.get("name_norm") or "").strip() or _find_norm(name)
        stem_norm = str(raw.get("stem_norm") or "").strip()
        if not stem_norm:
            stem_norm = _find_norm(Path(name).stem if not is_dir else name)
        rel_norm = str(raw.get("rel_norm") or "").strip() or _find_norm(rel)
        parent_norm = str(raw.get("parent_norm") or "").strip() or _find_norm(Path(rel).parent.as_posix())
        return {
            "path": p,
            "path_norm": str(raw.get("path_norm") or "").strip() or self._norm_abs_path_str(p),
            "root_name": root_name,
            "root_path": root_path,
            "rel": rel,
            "name": name,
            "is_dir": is_dir,
            "name_norm": name_norm,
            "stem_norm": stem_norm,
            "rel_norm": rel_norm,
            "parent_norm": parent_norm,
        }

    def _load_find_index_from_disk(self, force: bool = False) -> bool:
        path = self._find_index_path
        if not path.exists():
            return False
        try:
            st = path.stat()
            mtime_ns = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000)))
        except Exception:
            mtime_ns = 0

        with self._find_index_lock:
            if (not force) and self._find_index_loaded and (mtime_ns > 0) and (mtime_ns == self._find_index_mtime_ns):
                return True

        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
            raw_entries = obj.get("entries") if isinstance(obj, dict) else None
            if not isinstance(raw_entries, list):
                self._log_warning("普通 /find 索引读取失败：entries 非列表，回退实时扫描")
                return False
            normalized: List[dict] = []
            for item in raw_entries:
                one = self._normalize_find_index_entry(item)
                if one is None:
                    continue
                normalized.append(one)
            with self._find_index_lock:
                self._find_index_entries = normalized
                self._find_index_mtime_ns = mtime_ns
                self._find_index_loaded = True
            self._log_info(f"普通 /find 索引已加载：条目={len(normalized)}")
            return True
        except Exception as e:
            self._log_warning(f"普通 /find 索引读取失败，回退实时扫描: {e}")
            return False

    def ensure_find_index_loaded(self) -> bool:
        return self._load_find_index_from_disk(force=False)

    def build_find_index(self) -> Dict[str, int]:
        bases = self._iter_find_index_bases()
        self._log_info("普通 /find 索引：开始构建")

        entries: List[dict] = []
        seen = set()
        scanned_dirs = 0
        scanned_files = 0

        for root_name, base in bases:
            if not base.exists() or (not base.is_dir()):
                continue
            for root, dirs, files in os.walk(base):
                dirs.sort(key=lambda s: s.lower())
                dirs[:] = [d for d in dirs if self._find_dir_is_walkable(d)]
                files.sort(key=lambda s: s.lower())
                root_p = Path(root)

                for dn in dirs:
                    if not self._find_dir_is_searchable(dn):
                        continue
                    p = root_p / dn
                    try:
                        key = os.path.normcase(str(p.resolve()))
                    except (OSError, RuntimeError):
                        key = os.path.normcase(str(p))
                    if key in seen:
                        continue
                    seen.add(key)
                    try:
                        rel = p.relative_to(base).as_posix()
                    except ValueError:
                        rel = p.name
                    entries.append(
                        {
                            "path": str(p.resolve() if p.exists() else p),
                            "path_norm": self._norm_abs_path_str(p),
                            "root_name": root_name,
                            "root_path": str(base.resolve() if base.exists() else base),
                            "rel": rel,
                            "name": p.name,
                            "is_dir": True,
                            "name_norm": _find_norm(p.name),
                            "stem_norm": _find_norm(p.name),
                            "rel_norm": _find_norm(rel),
                            "parent_norm": _find_norm(Path(rel).parent.as_posix()),
                        }
                    )
                    scanned_dirs += 1

                for fn in files:
                    p = root_p / fn
                    if not self._find_file_is_searchable(p):
                        continue
                    try:
                        key = os.path.normcase(str(p.resolve()))
                    except (OSError, RuntimeError):
                        key = os.path.normcase(str(p))
                    if key in seen:
                        continue
                    seen.add(key)
                    try:
                        rel = p.relative_to(base).as_posix()
                    except ValueError:
                        rel = p.name
                    entries.append(
                        {
                            "path": str(p.resolve() if p.exists() else p),
                            "path_norm": self._norm_abs_path_str(p),
                            "root_name": root_name,
                            "root_path": str(base.resolve() if base.exists() else base),
                            "rel": rel,
                            "name": p.name,
                            "is_dir": False,
                            "name_norm": _find_norm(p.name),
                            "stem_norm": _find_norm(Path(p.name).stem),
                            "rel_norm": _find_norm(rel),
                            "parent_norm": _find_norm(Path(rel).parent.as_posix()),
                        }
                    )
                    scanned_files += 1

        payload = {"version": 1, "generated_ts": int(time.time()), "entries": entries}
        self._safe_write_json_atomic(self._find_index_path, payload)
        mtime_ns = 0
        try:
            st = self._find_index_path.stat()
            mtime_ns = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000)))
        except Exception:
            mtime_ns = 0

        with self._find_index_lock:
            self._find_index_entries = entries
            self._find_index_mtime_ns = mtime_ns
            self._find_index_loaded = True

        self._log_info(
            f"普通 /find 索引：构建完成 (目录={scanned_dirs}, 文件={scanned_files}, 条目={len(entries)})"
        )
        return {"dirs": scanned_dirs, "files": scanned_files, "entries": len(entries)}

    def _ctx_roots(self, ctx) -> List[Root]:
        out = [r for r in self.roots if ctx.level >= r.min_level]

        # 管理员可直接浏览整个 groups/（所有群的资料）
        if ctx.level >= 3:
            out.append(Root("groups", GROUP_DOCS_DIR, 3))

        # 群专属目录：只有 level>=1 且有 group_id 的场景开放（更符合你“group 目录”直觉）
        if ctx.group_id is not None and ctx.level >= 1:
            out.append(Root("group", GROUP_DOCS_DIR / str(ctx.group_id), 1))

        # （可选）个人专属目录：暂时不对外暴露，避免和你的“只用四个子文件夹”目标冲突
        # out.append(Root("me", USER_DOCS_DIR / str(ctx.user_id), 1))

        # 去重（按 name）
        uniq = {}
        for r in out:
            uniq[r.name] = r
        return list(uniq.values())

    def _pick_root(self, ctx, root_name: str) -> Optional[Root]:
        for r in self._ctx_roots(ctx):
            if r.name == root_name:
                if ctx.level >= r.min_level:
                    r.path.mkdir(parents=True, exist_ok=True)
                    return r
        return None

    def _safe_join(self, base: Path, sub: str) -> Optional[Path]:
        # 防止 ../ 穿越
        p = (base / sub).resolve()
        try:
            base_res = base.resolve()
            p.relative_to(base_res)
            return p
        except (OSError, RuntimeError, ValueError):
            return None

    def list_dir(self, ctx, arg: Optional[str]) -> Tuple[bool, str]:
        roots = self._ctx_roots(ctx)

        if not arg:
            names = [r.name + "/" for r in roots]
            names = sorted(set(names))
            return True, "可浏览目录：\n" + "\n".join(f"- {n}" for n in names)

        arg = arg.strip().strip("/")
        parts = arg.split("/", 1)
        root_name = parts[0]
        sub = parts[1] if len(parts) == 2 else ""

        r = self._pick_root(ctx, root_name)
        if not r:
            return False, f"无权限或不存在的根目录：{root_name}/"

        target = self._safe_join(r.path, sub) if sub else r.path.resolve()
        if not target or not target.exists():
            return False, "目录不存在"

        if not target.is_dir():
            return False, "这不是目录"

        entries = []
        for name in os.listdir(target):
            p = target / name
            entries.append((p.is_dir(), name))

        entries.sort(key=lambda x: (not x[0], x[1].lower()))
        entries = entries[:LS_LIMIT]

        lines = []
        for is_dir, name in entries:
            lines.append(("📁 " if is_dir else "📄 ") + (name + ("/" if is_dir else "")))

        if not lines:
            return True, "目录为空"
        return True, "目录内容：\n" + "\n".join(lines)

    def _expand_find_term(self, term: str) -> List[str]:
        variants = [term]

        aliases = _FIND_ALIAS_MAP.get(term)
        if aliases:
            variants.extend(aliases)

        # Strip common trailing words to improve tolerance for "xxx部分/资料/课件" queries.
        for suffix in ("部分", "资料", "课件", "题库", "答案", "试题", "复习", "总结", "教材"):
            suf = _find_norm(suffix)
            if term.endswith(suf) and len(term) > len(suf) + 1:
                variants.append(term[: -len(suf)])

        return _dedupe_keep_order(variants)[:10]

    def _prepare_find_query(self, keyword: str) -> Tuple[str, List[List[str]]]:
        raw = unicodedata.normalize("NFKC", str(keyword or "")).strip()
        compact = _find_norm(raw)
        if not compact:
            return "", []

        tokens = [_find_norm(x) for x in _FIND_SPLIT_RE.split(raw) if _find_norm(x)]
        tokens = _dedupe_keep_order(tokens)

        # Multi-token query: treat each token as one term group.
        # Single-token query: keep as one group.
        base_terms = tokens if len(tokens) >= 2 else [tokens[0] if tokens else compact]
        groups = [self._expand_find_term(t) for t in base_terms if t]
        if not groups:
            groups = [[compact]]

        return compact, groups

    @staticmethod
    def _subsequence_compactness(needle: str, haystack: str) -> float:
        if (not needle) or (not haystack) or (len(needle) > len(haystack)):
            return 0.0

        n = len(needle)
        j = 0
        first = -1
        last = -1
        for i, ch in enumerate(haystack):
            if ch == needle[j]:
                if first < 0:
                    first = i
                last = i
                j += 1
                if j >= n:
                    break
        if j < n or first < 0 or last < first:
            return 0.0

        span = last - first + 1
        if span <= 0:
            return 0.0
        return float(n) / float(span)

    @staticmethod
    def _char_dice(a: str, b: str) -> float:
        if (not a) or (not b):
            return 0.0
        sa = set(a)
        sb = set(b)
        inter = len(sa & sb)
        if inter <= 0:
            return 0.0
        return (2.0 * float(inter)) / float(len(sa) + len(sb))

    def _score_term(self, term: str, text_norm: str) -> float:
        if (not term) or (not text_norm):
            return 0.0

        if term == text_norm:
            return 260.0
        if text_norm.startswith(term):
            ratio = min(1.0, float(len(term)) / float(max(1, len(text_norm))))
            return 210.0 + 25.0 * ratio
        if term in text_norm:
            ratio = min(1.0, float(len(term)) / float(max(1, len(text_norm))))
            return 175.0 + 25.0 * ratio

        score = 0.0

        compact = self._subsequence_compactness(term, text_norm)
        if compact > 0.0:
            if len(term) <= 2:
                if compact >= 0.5:
                    score = max(score, 120.0 + 35.0 * compact)
            else:
                score = max(score, 95.0 + 45.0 * compact)

        dice = self._char_dice(term, text_norm)
        if len(term) <= 2:
            if dice >= 0.9:
                score = max(score, 120.0 + 30.0 * dice)
        else:
            if dice >= 0.55:
                score = max(score, 80.0 + 60.0 * dice)

        return score

    @staticmethod
    def _depth_under(base: Path, p: Path) -> int:
        try:
            return len(p.relative_to(base).parts)
        except ValueError:
            return len(p.parts)

    def _score_candidate(self, p: Path, base: Path, is_file: bool, query_compact: str, query_groups: List[List[str]]) -> float:
        name = p.name
        stem = Path(name).stem if is_file else name
        try:
            rel = p.relative_to(base).as_posix()
        except ValueError:
            rel = name

        forms = []
        name_norm = _find_norm(name)
        stem_norm = _find_norm(stem)
        rel_norm = _find_norm(rel)
        if name_norm:
            forms.append((name_norm, 1.00))
        if stem_norm and stem_norm != name_norm:
            forms.append((stem_norm, 1.00))
        if rel_norm and rel_norm not in (name_norm, stem_norm):
            forms.append((rel_norm, 0.75))
        if not forms:
            return 0.0

        term_scores: List[float] = []
        for variants in query_groups:
            best = 0.0
            for term in variants:
                for form, weight in forms:
                    s = self._score_term(term, form) * weight
                    if s > best:
                        best = s
            term_scores.append(best)

        if not term_scores:
            return 0.0

        # Require enough strong matches for multi-token queries.
        strong_cut = 125.0 if len(query_compact) <= 2 else 90.0
        strong_hits = sum(1 for s in term_scores if s >= strong_cut)
        if strong_hits <= 0:
            return 0.0
        if len(term_scores) >= 2 and strong_hits < max(1, len(term_scores) // 2):
            return 0.0

        avg_score = sum(term_scores) / float(len(term_scores))
        best_score = max(term_scores)
        final_score = (avg_score * 0.72) + (best_score * 0.28) + (6.0 * strong_hits)
        if strong_hits == len(term_scores):
            final_score += 12.0

        # Phrase bonus for multi-token query.
        if len(query_groups) >= 2 and query_compact:
            phrase_best = 0.0
            for form, weight in forms:
                phrase_best = max(phrase_best, self._score_term(query_compact, form) * weight)
            if phrase_best > 0.0:
                final_score += phrase_best * 0.22

        return final_score

    @staticmethod
    def _min_find_score(query_compact: str, query_groups: List[List[str]]) -> float:
        qlen = len(query_compact)
        if qlen <= 1:
            return 210.0
        if qlen == 2:
            return 128.0
        if qlen <= 4:
            return 96.0
        if len(query_groups) >= 2:
            return 88.0
        return 82.0

    def _find_by_scan(
        self,
        *,
        query_compact: str,
        query_groups: List[List[str]],
        min_score: float,
        search_bases: List[Path],
    ) -> List[Path]:
        dir_hits_scored: List[Tuple[float, int, str, Path]] = []
        file_hits_scored: List[Tuple[float, int, str, Path]] = []
        seen = set()  # Deduplicate overlapping roots (e.g. groups/ and group/).
        scanned = 0
        stop_scan = False

        for base in search_bases:
            if stop_scan:
                break
            for root, dirs, files in os.walk(base):
                dirs.sort(key=lambda s: s.lower())
                dirs[:] = [d for d in dirs if self._find_dir_is_walkable(d)]
                files.sort(key=lambda s: s.lower())
                root_p = Path(root)

                for dn in dirs:
                    if not self._find_dir_is_searchable(dn):
                        continue
                    scanned += 1
                    if scanned > FIND_MAX_SCAN:
                        stop_scan = True
                        break
                    p = root_p / dn
                    score = self._score_candidate(p, base, False, query_compact, query_groups)
                    if score < min_score:
                        continue
                    try:
                        key = os.path.normcase(str(p.resolve()))
                    except (OSError, RuntimeError):
                        key = os.path.normcase(str(p))
                    if key in seen:
                        continue
                    seen.add(key)
                    depth = self._depth_under(base, p)
                    dir_hits_scored.append((score, depth, dn.casefold(), p))

                if stop_scan:
                    break

                for fn in files:
                    p = root_p / fn
                    if not self._find_file_is_searchable(p):
                        continue
                    scanned += 1
                    if scanned > FIND_MAX_SCAN:
                        stop_scan = True
                        break
                    score = self._score_candidate(p, base, True, query_compact, query_groups)
                    if score < min_score:
                        continue
                    try:
                        key = os.path.normcase(str(p.resolve()))
                    except (OSError, RuntimeError):
                        key = os.path.normcase(str(p))
                    if key in seen:
                        continue
                    seen.add(key)
                    depth = self._depth_under(base, p)
                    file_hits_scored.append((score, depth, fn.casefold(), p))
                if stop_scan:
                    break

        dir_hits_scored.sort(key=lambda x: (-x[0], x[1], x[2]))
        file_hits_scored.sort(key=lambda x: (-x[0], x[1], x[2]))
        file_hits = [x[3] for x in file_hits_scored[:FIND_FILE_LIMIT]]
        dir_hits = [x[3] for x in dir_hits_scored[:FIND_DIR_LIMIT]]
        return file_hits + dir_hits

    def _find_with_index(
        self,
        *,
        query_compact: str,
        query_groups: List[List[str]],
        min_score: float,
        search_bases: List[Path],
    ) -> List[Path]:
        if not self.ensure_find_index_loaded():
            raise FileNotFoundError("find index not ready")

        base_norm_pairs: List[Tuple[Path, str]] = []
        for base in search_bases:
            try:
                resolved = base.resolve()
            except Exception:
                resolved = base
            base_norm_pairs.append((resolved, self._norm_abs_path_str(resolved)))

        with self._find_index_lock:
            entries = list(self._find_index_entries)

        dir_hits_scored: List[Tuple[float, int, str, Path]] = []
        file_hits_scored: List[Tuple[float, int, str, Path]] = []
        seen = set()

        for item in entries:
            p_raw = str((item or {}).get("path") or "").strip()
            if not p_raw:
                continue
            p = Path(p_raw)
            p_norm = str((item or {}).get("path_norm") or "").strip() or self._norm_abs_path_str(p_raw)

            matched_base: Optional[Path] = None
            for base, base_norm in base_norm_pairs:
                if self._path_under_norm(base_norm, p_norm):
                    matched_base = base
                    break
            if matched_base is None:
                continue

            is_dir = bool((item or {}).get("is_dir"))
            is_file = not is_dir
            score = self._score_candidate(p, matched_base, is_file, query_compact, query_groups)
            if score < min_score:
                continue
            if not p.exists():
                continue
            try:
                key = os.path.normcase(str(p.resolve()))
            except (OSError, RuntimeError):
                key = os.path.normcase(str(p))
            if key in seen:
                continue
            seen.add(key)
            depth = self._depth_under(matched_base, p)
            if is_dir:
                dir_hits_scored.append((score, depth, p.name.casefold(), p))
            else:
                file_hits_scored.append((score, depth, p.name.casefold(), p))

        dir_hits_scored.sort(key=lambda x: (-x[0], x[1], x[2]))
        file_hits_scored.sort(key=lambda x: (-x[0], x[1], x[2]))
        file_hits = [x[3] for x in file_hits_scored[:FIND_FILE_LIMIT]]
        dir_hits = [x[3] for x in dir_hits_scored[:FIND_DIR_LIMIT]]
        return file_hits + dir_hits

    def find(self, ctx, keyword: str, in_dir: Optional[str] = None) -> List[Path]:
        keyword = (keyword or "").strip()
        if not keyword:
            return []

        query_compact, query_groups = self._prepare_find_query(keyword)
        if not query_compact or not query_groups:
            return []
        min_score = self._min_find_score(query_compact, query_groups)

        roots = self._ctx_roots(ctx)

        # Optional: limit search under a specific directory, e.g. /find analog public/textbook_and_material
        base_filters: List[Path] = []
        if in_dir:
            in_dir = in_dir.strip().strip("/")
            parts = in_dir.split("/", 1)
            r = self._pick_root(ctx, parts[0])
            if r:
                sub = parts[1] if len(parts) == 2 else ""
                target = self._safe_join(r.path, sub) if sub else r.path
                if target and target.exists() and target.is_dir():
                    base_filters = [target]

        search_bases = base_filters if base_filters else [r.path for r in roots]
        if not search_bases:
            return []

        try:
            return self._find_with_index(
                query_compact=query_compact,
                query_groups=query_groups,
                min_score=min_score,
                search_bases=search_bases,
            )
        except FileNotFoundError:
            return self._find_by_scan(
                query_compact=query_compact,
                query_groups=query_groups,
                min_score=min_score,
                search_bases=search_bases,
            )
        except Exception as e:
            self._log_warning(f"普通 /find 索引不可用，回退实时扫描: {e}")
            return self._find_by_scan(
                query_compact=query_compact,
                query_groups=query_groups,
                min_score=min_score,
                search_bases=search_bases,
            )

    def display_rel(self, p: Path) -> str:
        """展示用：尽量显示相对 data/ 的路径（POSIX 风格）。"""
        try:
            rel = p.resolve().relative_to(DATA_DIR.resolve())
            return rel.as_posix()
        except (OSError, RuntimeError, ValueError):
            return p.name

    def to_container_path(self, p: Path) -> str:
        """把宿主机 data/ 下的文件路径映射到 NapCat 容器内挂载路径。"""
        rel = p.resolve().relative_to(DATA_DIR.resolve())
        return str(PurePosixPath(DATA_DIR_CONTAINER) / PurePosixPath(rel.as_posix()))
