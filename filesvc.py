# filesvc.py
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import List, Optional, Tuple
import os

from config import (
    DOC_ROOTS,
    GROUP_DOCS_DIR,
    USER_DOCS_DIR,
    HANDIN_ROOT_DIR,
    UPLOAD_GROUP_HOST_DIR,
    UPLOAD_PRIVATE_HOST_DIR,
    LS_LIMIT,
    FIND_LIMIT,
    FIND_MAX_SCAN,
    DATA_DIR,
    DATA_DIR_CONTAINER,
)

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
        except Exception:
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

    def find(self, ctx, keyword: str, in_dir: Optional[str] = None) -> List[Path]:
        keyword = (keyword or "").strip()
        if not keyword:
            return []

        roots = self._ctx_roots(ctx)

        # 可选：限制在某个目录里搜，例如 /find 模电 public/电路
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

        hits: List[Path] = []
        seen = set()  # 去重：避免 admin 同时扫描 groups/ 与 group/ 时重复命中
        scanned = 0
        kw_low = keyword.lower()

        for base in search_bases:
            for root, dirs, files in os.walk(base):
                dirs.sort(key=lambda s: s.lower())
                files.sort(key=lambda s: s.lower())

                for dn in dirs:
                    scanned += 1
                    if scanned > FIND_MAX_SCAN:
                        return hits[:FIND_LIMIT]
                    if kw_low in dn.lower():
                        p = Path(root) / dn
                        try:
                            key = os.path.normcase(str(p.resolve()))
                        except Exception:
                            key = os.path.normcase(str(p))
                        if key in seen:
                            continue
                        seen.add(key)
                        hits.append(p)
                        if len(hits) >= FIND_LIMIT:
                            return hits

                for fn in files:
                    scanned += 1
                    if scanned > FIND_MAX_SCAN:
                        return hits[:FIND_LIMIT]
                    if kw_low in fn.lower():
                        p = Path(root) / fn
                        try:
                            key = os.path.normcase(str(p.resolve()))
                        except Exception:
                            key = os.path.normcase(str(p))
                        if key in seen:
                            continue
                        seen.add(key)
                        hits.append(p)
                        if len(hits) >= FIND_LIMIT:
                            return hits
        return hits

    def display_rel(self, p: Path) -> str:
        """展示用：尽量显示相对 data/ 的路径（POSIX 风格）。"""
        try:
            rel = p.resolve().relative_to(DATA_DIR.resolve())
            return rel.as_posix()
        except Exception:
            return p.name

    def to_container_path(self, p: Path) -> str:
        """把宿主机 data/ 下的文件路径映射到 NapCat 容器内挂载路径。"""
        rel = p.resolve().relative_to(DATA_DIR.resolve())
        return str(PurePosixPath(DATA_DIR_CONTAINER) / PurePosixPath(rel.as_posix()))
