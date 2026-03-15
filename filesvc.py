# filesvc.py
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional, Set, Tuple
import os
import re
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
    def __init__(self):
        self.roots: List[Root] = [Root(n, Path(p), int(lv)) for (n, p, lv) in DOC_ROOTS]

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
                files.sort(key=lambda s: s.lower())
                root_p = Path(root)

                for dn in dirs:
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
                    scanned += 1
                    if scanned > FIND_MAX_SCAN:
                        stop_scan = True
                        break
                    p = root_p / fn
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
        dir_hits = [x[3] for x in dir_hits_scored[:FIND_DIR_LIMIT]]
        file_hits = [x[3] for x in file_hits_scored[:FIND_FILE_LIMIT]]
        return dir_hits + file_hits

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
